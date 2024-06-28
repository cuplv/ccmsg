{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm
  ( CcmT
  , getSelf
  , getOthers
  , SendTarget (..)
  , sendCcm
  , sendToCcm
  -- , sendCcm'
  -- , recvCcm
  -- , recvCcm'
  , recvManyCcm
  , recvManyCcmA
  , recvManyCcmB
  , allReadyCcm
  , awaitAllSent
  , runCcm
  , stmCcm
  , atomicallyCcm
  , atomicallyCcmTimedMicros
  , orElseCcm
  , RClock
  , zeroRClock
  , MyAddr (..)
  , NodeId (..)
  , nodeId
  , CausalError (..)
  , showCausalError'
  , Debugger
  , mkIODbg
  , mkPrinterDbg
  , mkNoDebugDbg
  , runQD
  , runQD'
  , debug
  , liftIO
  ) where

import Network.Ccm.Bsm
import Network.Ccm.Lens
import Network.Ccm.State
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Concurrent (forkIO, threadDelay, killThread)
import Control.Concurrent.STM
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (toList,foldMap,foldlM)
import Data.Map (Map)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Store as Store
import Data.Traversable (for)

{-| Representation of causal dependencies. -}
data RClock = RClock VClock

remoteClock :: NodeId -> VClock -> RClock
remoteClock i v = RClock $ deleteVC i v

{-| @'completeClock' i vc rc@ adds the @i@ record of @vc@ to @rc@ -}
completeClock :: NodeId -> VClock -> RClock -> VClock
completeClock i v1 (RClock v2) = case lookupVC i v1 of
  Just n -> tickBy (n + 1) i v2
  Nothing -> v2

{-| Represents no dependencies. -}
zeroRClock :: RClock
zeroRClock = RClock zeroClock

type CcmT m = ReaderT BsmMulti (CcmST m)

getSelf :: (Monad m) => CcmT m NodeId
getSelf = getBsmSelfId <$> ask

getOthers :: (Monad m) => CcmT m (Set NodeId)
getOthers = getRemoteNodeIds <$> ask

{-| Send a message, using the most recent local clock.  This has the
  potential to create causal dependencies that are stricter (less
  efficient) than necessary.  'sendCcm'' is potentially more
  efficient. -}
sendCcm :: ByteString -> CcmT STM ()
sendCcm = sendToCcm SendAll

{-| Send a message to a specific target.  If causal consistency is being
  enforced, this results in a broadcast anyway. -}
sendToCcm :: SendTarget -> ByteString -> CcmT STM ()
sendToCcm target content = do
  causal <- use ccmEnforceCausal
  if causal
    then do
      -- Use local clock for message.
      local <- lift $ use localClock
      encodeAndBcastCausal (local,content)
    else do
      encodeAndMcast target content

-- {-| Send a message.  The 'RClock' argument describes the message's
--   dependencies.  It should be the last 'RClock' received from
--   'recvCcm'', or else 'zeroRClock' if no messages have yet
--   been sent. -}
-- sendCcm' :: (RClock, ByteString) -> CcmT STM ()
-- sendCcm' (rc,content) = do
--   bsm <- ask
--   let self = getBsmSelfId bsm
--   local <- lift $ use localClock

--   -- "Complete" the provided remote-clock by adding the local node's
--   -- events.
--   let clock = completeClock self local rc

--   -- Broadcast the message.
--   encodeAndBcastCausal (clock,content)

encodeAndMcast :: SendTarget -> ByteString -> CcmT STM ()
encodeAndMcast target content = do
  bsm <- ask
  let self = getBsmSelfId bsm
  let msg = mkSimpleAppMsg content
  -- Don't need to record sending the message
  lift.lift $ sendBsm bsm target (Store.encode (msg :: SimpleAppMsg))

encodeAndBcastCausal :: (VClock, ByteString) -> CcmT STM ()
encodeAndBcastCausal (clock,appContent) = do
  bsm <- ask
  let self = getBsmSelfId bsm
  let msg = mkCausalAppMsg clock appContent
  lift $ recordSend (self,msg)
  lift.lift $ sendBsm bsm SendAll (Store.encode (msg :: AppMsg))

-- {-| Receive messages from the causal message service.  A non-empty list
--   of 'ByteString' messages is returned. -}
-- recvCcm :: CcmT STM (Either CausalError [ByteString])
-- recvCcm = (fmap snd) <$> recvCcm'

-- {-| Receive messages from the causal message service.  A non-empty list
--   of 'ByteString' messages is returned, along with a clock value that
--   includes them, and their causal dependencies.

--   Note that the returned message list may be empty, in case messages
--   were received from the network but could not be causally delivered
--   yet. -}
-- recvCcm' :: CcmT STM (Either CausalError (RClock, [ByteString]))
-- recvCcm' = do
--   bsm <- ask
--   let self = getBsmSelfId bsm
--   (sender,rawContent) <- lift.lift $ getFromInbox bsm
--   let msg = case Store.decode rawContent of
--         Right m -> m
--         Left e -> error $ "Decode fail in recvCcm'" ++ show e
--   result <- lift $ tryDeliver (sender,msg)
--   case result of
--     Right (local, dmsgs) ->
--       return $ Right (remoteClock self local, toList dmsgs)
--     Left e ->
--       return $ Left e
--   -- (local, dmsgs) <- lift $ tryDeliver self (sender,msg)
--   -- return $ Right (remoteClock self local, dmsgs)

{-| Decode and deliver a simple (non-causal) message. -}
handleSimpleMsg
  :: (MonadIO m)
  => (NodeId, ByteString)
  -> CcmT m ByteString
handleSimpleMsg (sender, rawContent) =
  case Store.decode rawContent of
    Right (SimpleAppMsg msg) -> return msg
    Left e -> case Store.decode rawContent of
      Right msg ->
        let
          cmsg = (msg :: AppMsg)
        in
          error $
            "Got causal msg when expecting simple msg, from "
            ++ show sender
      Left _ ->
        error $
          "Decode failure in handleSimpleMsg, from "
          ++ show sender
          ++ ", "
          ++ show e

{-| Decode and attempt to deliver a causal message, possibly delivering
  additional retried messages as a result. -}
handleCausalMsg
  :: (MonadIO m)
  => (NodeId, ByteString)
  -> CcmT m (Either CausalError (Seq ByteString))
handleCausalMsg (sender, rawContent) = do
  let msg = case Store.decode rawContent of
        Right msg -> (msg :: AppMsg)
        Left e -> case Store.decode rawContent of
          Right other ->
            let
              smesg = (other :: SimpleAppMsg)
            in
              error $
                "Got simple msg when expecting causal msg, from "
                ++ show sender
          Left _ ->
            error $
              "Decode failure in handleCausalMsg, from "
              ++ show sender
              ++ ", "
              ++ show e
  result <- lift $ tryDeliver (sender,msg)
  case result of
    Right (_, dmsgs) ->
      return $ Right dmsgs
    Left e ->
      return $ Left e

recvManyCcmA :: CcmT STM [(NodeId, ByteString)]
recvManyCcmA = do
  bsm <- ask
  stmCcm $ getManyFromInbox bsm

recvManyCcmB
  :: (MonadIO m)
  => [(NodeId, ByteString)]
  -> CcmT m (Int, Seq ByteString)
recvManyCcmB rawMsgs = do
  causal <- use ccmEnforceCausal
  let h (ds,ms) raw | not causal = do
        m <- handleSimpleMsg raw
        return (ds, ms Seq.|> m)
      h (ds,ms) raw | causal = do
        result <- handleCausalMsg raw
        case result of
          Right ms' -> return (ds, ms Seq.>< ms')
          Left e -> return (ds + 1, ms)
  foldlM h (0, Seq.empty) rawMsgs

recvManyCcm :: (MonadIO m) => CcmT m (Int, Seq ByteString)
recvManyCcm = atomicallyCcm recvManyCcmA >>= recvManyCcmB
-- recvManyCcm = do
--   bsm <- ask
--   rawMsgs <- liftIO . atomically $ getManyFromInbox bsm
--   causal <- use ccmEnforceCausal
--   let h (ds,ms) raw | not causal = do
--         m <- handleSimpleMsg raw
--         return (ds, ms Seq.|> m)
--       h (ds,ms) raw | causal = do
--         result <- handleCausalMsg raw
--         case result of
--           Right ms' -> return (ds, ms Seq.>< ms')
--           Left e -> return (ds + 1, ms)
--   foldlM h (0, Seq.empty) rawMsgs

{-| Check that all nodes are connected and ready to receive messages. -}
allReadyCcm :: CcmT STM Bool
allReadyCcm = do
  bsm <- ask
  lift.lift $ allReady bsm

{-| Run a 'CcmT' computation.  This needs 'IO' in order to operate TCP
  connections and such. -}
runCcm
  :: (MonadIO m)
  => Debugger
  -> Bool -- ^ Whether to enforce causal consistency
  -> NodeId -- ^ The local node's ID.
  -> Map NodeId MyAddr -- ^ Addresses of all nodes.
  -> CcmT m a -> m a
runCcm d causal self addrs comp = do
  bsm <- liftIO $ runBsmMulti d self addrs
  evalStateT (runReaderT comp bsm) (newCcmState causal)

{-| Lift an 'STM' transaction into the 'CcmT' monad. -}
stmCcm :: STM a -> CcmT STM a
stmCcm = lift.lift

{-| Transform a 'STM'-based computation into an 'IO'-based computation. -}
atomicallyCcm :: (MonadIO m) => CcmT STM a -> CcmT m a
atomicallyCcm m = do
  bsm <- ask
  s1 <- lift $ use id
  let stm = runStateT (runReaderT m bsm) s1
  (a,s2) <- lift.lift.liftIO.atomically $ stm
  lift $ id .= s2
  return a

{-| Transform a 'STM'-based computation into an 'IO'-based computation,
  which will cancel after the given number of microseconds. -}
atomicallyCcmTimedMicros :: (MonadIO m) => Maybe Int -> CcmT STM a -> CcmT m (Maybe a)
atomicallyCcmTimedMicros mms m = case mms of
  Just micros -> do
    v <- liftIO $ newEmptyTMVarIO
    tid <- liftIO . forkIO $ do
      threadDelay micros
      atomically $ putTMVar v ()

    bsm <- ask
    s1 <- lift $ use id
    let stm =
          (Just <$> runStateT (runReaderT m bsm) s1)
          `orElse`
          (const Nothing <$> takeTMVar v)
    result <- lift.lift.liftIO.atomically $ stm
    case result of
      Just (a,s2) -> do
        liftIO $ killThread tid
        lift $ id .= s2
        return $ Just a
      Nothing -> return Nothing
  Nothing -> Just <$> atomicallyCcm m

{-| Try the first transaction.  If it calls 'retry', do the second one
  instead.  This is a lifted version of 'orElse' from 'STM'. -}
orElseCcm
  :: CcmT STM a
  -> CcmT STM a
  -> CcmT STM a
orElseCcm m1 m2 = do
  bsm <- ask
  s1 <- lift $ use id
  let
    stm1 = runStateT (runReaderT m1 bsm) s1
    stm2 = runStateT (runReaderT m2 bsm) s1
  (a,s2) <- lift.lift $ orElse stm1 stm2
  lift $ id .= s2
  return a

{-| Blocks until send-queue is empty.  Call this before shutting down,
  in order to avoid sent messages getting cut off before they are
  actually transmitted. -}
awaitAllSent :: CcmT STM ()
awaitAllSent = do
  bsm <- ask
  stmCcm $ check =<< checkAllSent bsm
