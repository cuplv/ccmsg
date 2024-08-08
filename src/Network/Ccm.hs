{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm
  ( CcmT
  , getSelf
  , getOthers
  , blockSend
  , blockRecv
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

{-| Send a message, which causally follows all messages received so-far.

  This function may block if the outgoing transport queue is full.
-}
blockSend :: ByteString -> CcmT STM ()
blockSend content = do
  local <- lift $ use localClock
  encodeAndBcast (local,content)

encodeAndBcast :: (VClock, ByteString) -> CcmT STM ()
encodeAndBcast (clock,appContent) = do
  bsm <- ask
  let self = getBsmSelfId bsm
  let msg = mkCausalAppMsg clock appContent
  lift $ recordSend (self,msg)
  lift.lift $ sendBsm bsm SendAll (Store.encode (msg :: AppMsg))

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
    Right (_, dmsgs) -> do
      ccmStats . totalInOrder += 1
      return $ Right dmsgs
    Left e -> do
      ccmStats . totalOutOfOrder += 1
      return $ Left e

{-| Receive casually-ordered messages.

  This function blocks until at least one message is available.  To
  avoid blocking, first check that messages are available using
  'inboxEmpty'.
-}
blockRecv :: (MonadIO m) => CcmT m (Seq ByteString)
blockRecv = do
  bsm <- ask
  rawMsgs <- liftIO . atomically $ getManyFromInbox bsm
  let h ms raw = do
        result <- handleCausalMsg raw
        case result of
          Right ms' -> return (ms Seq.>< ms')
          Left e -> return ms
  foldlM h Seq.empty rawMsgs

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
  -> NodeId -- ^ The local node's ID.
  -> Map NodeId MyAddr -- ^ Addresses of all nodes.
  -> CcmT m a -> m a
runCcm d self addrs comp = do
  bsm <- liftIO $ runBsmMulti d self addrs
  evalStateT (runReaderT comp bsm) newCcmState

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

data Context
  = Context { ctxInboxEmpty :: STM Bool }

inboxEmpty :: Context -> STM Bool
inboxEmpty = ctxInboxEmpty

context :: (Monad m) => CcmT m Context
context = undefined
