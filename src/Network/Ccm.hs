{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm
  ( CcmT
  , getSelf
  , getOthers
  , blockSend
  , tryRecv
  , runCcm
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
  , Context
  , context
  , allPeersReady
  , newNetworkActivity
  , sendsComplete
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

{-| Receive any causally-ordered messages that are available.

  This function will not block.  To block until messages are
  available, use 'Control.Concurrent.STM.check' with
  'newNetworkActivity'.
-}
tryRecv :: (MonadIO m) => CcmT m (Seq ByteString)
tryRecv = do
  bsm <- ask
  rawMsgs <- liftIO . atomically $ tryGetManyFromInbox bsm
  let h ms raw = do
        result <- handleCausalMsg raw
        case result of
          Right ms' -> return (ms Seq.>< ms')
          Left e -> return ms
  foldlM h Seq.empty rawMsgs

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

data Context
  = Context { ctxInboxEmpty :: STM Bool
            , ctxAllReady :: STM Bool
            , ctxOutboxEmpty :: STM Bool
            }

{-| Check that all peers are connected and ready to receive messages. -}
allPeersReady :: Context -> STM Bool
allPeersReady = ctxAllReady

{-| Check whether any messages are ready to be processed. -}
newNetworkActivity :: Context -> STM Bool
newNetworkActivity ctx = not <$> ctxInboxEmpty ctx

{-| Check whether any sent messages are still waiting to be transmitted. -}
sendsComplete :: Context -> STM Bool
sendsComplete = ctxOutboxEmpty

context :: (Monad m) => CcmT m Context
context = do
  bsm <- ask
  return $ Context
    { ctxInboxEmpty = isEmptyInbox bsm
    , ctxAllReady = allReady bsm
    , ctxOutboxEmpty = checkAllSent bsm
    }
