{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Internal where

import Control.Monad.DebugLog
import Network.Ccm.Bsm
import Network.Ccm.Lens
import qualified Network.Ccm.Sort as Sort
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Concurrent.STM
import Control.Monad (foldM)
import Control.Monad.Reader
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Store.TH (makeStore)
import qualified Data.Store as Store

data CcmConfig
  = CcmConfig
    { _cccTransmissionBatch :: PostCount
    }
  deriving (Show,Eq,Ord)

makeLenses ''CcmConfig

-- | A causal messaging post
data Post
  = Post
    { _postCreator :: NodeId
    , _postDeps :: VClock
    , _postContent :: ByteString
    }
  deriving (Show,Eq,Ord)

makeLenses ''Post
makeStore ''Post

seqNum :: SimpleGetter Post SeqNum
seqNum = to $ \r -> nextNum (r^.postCreator) (r^.postDeps)

data CcmState
  = CcmState
    { _sortState :: Sort.State
    , _sortOutput :: Seq (NodeId, ByteString)
      -- ^ Causal-sorted post contents
    , _ccmPostStore :: Map NodeId (PostCount, Seq Post)
      -- ^ The store of posts that can be transmitted.
    , _inputClock :: VClock
      -- ^ The clock of posts that have been received.
    , _knownClock :: VClock
      -- ^ The input clock, plus all posts that have been referenced
      -- in dependencies.
    , _ccmPeerClocks :: Map NodeId VClock
      -- ^ The clocks reported as received (and output) by peers.
    , _ccmPeerFrames :: Map NodeId SeqNum
      -- ^ The next local post that should be sent to each peer.
    , _peerRequests :: Map (NodeId, NodeId) SeqNum
      -- ^ @(i1,i2) -> sn@ means that we should send @i2@'s posts
      -- starting from @sn@ to @i1@.
    }

makeLenses ''CcmState

newCcmState :: CcmState
newCcmState = CcmState
  { _sortState = Sort.newState
  , _sortOutput = Seq.Empty
  , _ccmPostStore = Map.empty
  , _inputClock = zeroClock
  , _knownClock = zeroClock
  , _ccmPeerClocks = Map.empty
  , _ccmPeerFrames = Map.empty
  , _peerRequests = Map.empty
  }

peerClock :: NodeId -> Lens' CcmState VClock
peerClock i = ccmPeerClocks . at i . non zeroClock

postHistory :: NodeId -> Lens' CcmState (PostCount, Seq Post)
postHistory i = ccmPostStore . at i . non (0, Seq.Empty)

received :: NodeId -> SimpleGetter CcmState PostCount
received i = to $ \s ->
  let (count,posts) = s ^. postHistory i
  in count + fromIntegral (Seq.length posts)

peerFrame :: NodeId -> Lens' CcmState SeqNum
peerFrame i = ccmPeerFrames . at i . non 0

{- | Access a post by 'NodeId' and 'SeqNum'.  This will throw an error
   if the post has been garbage-collected or has not yet been
   received. -}
post :: NodeId -> SeqNum -> SimpleGetter CcmState Post
post i sn = to $ \s ->
  let
    (old,ps) = s ^. postHistory i
    x = fromIntegral sn - fromIntegral old
  in case Seq.lookup x ps of
    Just p -> p
    Nothing | x < 0 -> error $
      "Tried to access post that has been garbage collected ("
      ++ show i
      ++ ", "
      ++ show sn
      ++ ")"
    Nothing | x >= 0 -> error $
      "Tried to access post that is not yet received ("
      ++ show i
      ++ ", "
      ++ show sn
      ++ ")"

type CcmT m = ReaderT (CcmConfig, Bsm) (StateT CcmState m)

data CcmMsg
  = PostMsg Post
    -- ^ Post delivery: either a "primary" delivery from the post's
    -- creator or a "secondary" delivery from a different peer in
    -- response to a 'Retrans' request.
  | Backup SeqNum
    -- ^ Sender wants receiver to back up their own primary message
    -- delivery to start again with the given 'SeqNum'.
  | Retrans NodeId SeqNum
    -- ^ Sender wants the receiver to send a one-time sequence of all
    -- the messages it has seen from the given 'NodeId', starting with
    -- the given 'SeqNum'.
  | HeartBeat VClock
    -- ^ Notification that the sender has seen the given clock.
  deriving (Show,Eq,Ord)

makeStore ''CcmMsg

{- | Receive any messages from the network, handle them, and return any
   causal-ordered post contents. -}
tryRecv :: (MonadLog m, MonadIO m) => CcmT m (Seq (NodeId, ByteString))
tryRecv = do
  -- The sortOutput buffer should be empty before and after this
  -- function.

  -- Pull messages from receiver queue, and decode them.  Decoding
  -- errors produce a log message ("error"), and then are skipped.
  msgs <- tryReadMsgs
  -- Then handle each message in turn, collecting any causal-ordered
  -- posts that can be delivered to the application.
  for_ msgs (uncurry handleCcmMsg)
  -- Flush output buffer
  sortOutput <<.= Seq.Empty

tryReadMsgs :: (MonadLog m, MonadIO m) => CcmT m (Seq (NodeId, CcmMsg))
tryReadMsgs = do
  (_,bsm) <- ask
  rawMsgs <- liftIO . atomically $ tryReadInbox bsm
  let dc s (sender,bs) =
        case Store.decode bs of
          Right msg -> return $ s Seq.|> (sender,msg)
          Left e -> do
            dlog ["error"] $
              "Failed to decode msg from "
              ++ show sender
              ++ ": "
              ++ show e
            return s
  foldM dc Seq.Empty rawMsgs

handleCcmMsg
  :: (MonadLog m, MonadIO m)
  => NodeId
  -> CcmMsg
  -> CcmT m ()
handleCcmMsg sender = \case
  PostMsg p -> do
    pc <- use $ received sender
    if pc == p^.seqNum
      then do
        -- Deliver into store, input into sorter
        undefined
      else do
        -- Send @Backup pc@ command to sender
        undefined
  Backup sn -> do
    -- We assume that @sn@ is <= the number of local posts created so
    -- far.
    peerFrame sender .= sn
  Retrans i sn -> do
    peerRequests . at (sender,i) .= Just sn
  HeartBeat c -> do
    peerClock sender %= joinVC c

{- | Send any waiting messages, returning 'True' if there are more to
   send. -}
trySend :: (MonadLog m, MonadIO m) => CcmT m Bool
trySend = undefined

runCcm
  :: CcmConfig
  -> NodeId
  -> Map NodeId MyAddr
  -> CcmT (LogIO IO) a
  -> LogIO IO a
runCcm config self addrs comp = do
  bsm <- runBsm mkNoDebugDbg self addrs
  evalStateT (runReaderT comp (config,bsm)) newCcmState
