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
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Store.TH (makeStore)
import qualified Data.Store as Store

data CcmConfig
  = CcmConfig
    { _cccTransmissionBatch :: PostCount
    , _cccCacheMode :: CacheMode
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
    , _ccmPeerRequests :: Map (NodeId, NodeId) (SeqNum, SeqNum)
      -- ^ @(i1,i2) -> (sn1,sn2)@ means that we should send @i2@'s posts
      -- with sequence numbers starting at @sn1@ and ending at @sn2 - 1@.
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
  , _ccmPeerRequests = Map.empty
  }

peerClock :: NodeId -> Lens' CcmState VClock
peerClock i = ccmPeerClocks . at i . non zeroClock

postHistory :: NodeId -> Lens' CcmState (PostCount, Seq Post)
postHistory i = ccmPostStore . at i . non (0, Seq.Empty)

{- | Get a post from the store by its sender Id and sequence number.  If
   the post is not there, you get an error! -}
postById :: NodeId -> SeqNum -> SimpleGetter CcmState Post
postById i sn = to $ \s ->
  let (count,posts) = s ^. postHistory i
  in Seq.index posts (fromIntegral sn - fromIntegral count)

-- received :: NodeId -> SimpleGetter CcmState PostCount
-- received i = to $ \s ->
--   let (count,posts) = s ^. postHistory i
--   in count + fromIntegral (Seq.length posts)

storedPostRange :: NodeId -> SimpleGetter CcmState (SeqNum, PostCount)
storedPostRange i = to $ \s ->
  let (count,posts) = s ^. postHistory i
  in (count, fromIntegral (Seq.length posts))

received :: NodeId -> SimpleGetter CcmState PostCount
received i = to $ \s ->
  let (start,length) = s ^. storedPostRange i
  in start + length

peerFrame :: NodeId -> Lens' CcmState SeqNum
peerFrame i = ccmPeerFrames . at i . non 0

peerRequest :: NodeId -> NodeId -> Lens' CcmState (Maybe (SeqNum,SeqNum))
peerRequest i1 i2 = ccmPeerRequests . at (i1,i2)

openRequests :: SimpleGetter CcmState (Set (NodeId, NodeId))
openRequests = to $ \s -> Map.keysSet (s ^. ccmPeerRequests)


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
    -- ^ Notification that the sender has received the contents of the
    -- given clock.
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
    -- Check post store range for @i@.  We must have @sn@, and we will
    -- send up to the end of the store range.
    (start, length) <- use $ storedPostRange i
    if start <= sn && sn < (start + length)
      then peerRequest sender i .= Just (sn, start + length)
      else return ()
  HeartBeat c -> do
    peerClock sender %= joinVC c

sendLimit' :: (MonadLog m, MonadIO m) => Maybe PostCount -> CcmT m ()
sendLimit' mc = do
  self <- getSelf
  peers <- getPeers
  -- Send for each frame that is not maxed out.
  for_ peers $ \i -> do
    snF <- use $ peerFrame i
    snNext <- nextNum self <$> use inputClock
    if snF < snNext
      then undefined
      else undefined
  -- Send for each open retransmission request.
  undefined

{- | Send waiting messages, up to the transmission batch limit. -}
sendLimit :: (MonadLog m, MonadIO m) => CcmT m ()
sendLimit = do
  l <- view $ _1 . cccTransmissionBatch
  sendLimit' (Just l)

{- | Send all waiting messages, ignoring the transmission batch limit. -}
sendAll :: (MonadLog m, MonadIO m) => CcmT m ()
sendAll = sendLimit' Nothing

{- | Communicate with peers.

   This will handle any messages that have been received, and will
   send any waiting messages (up to the transmission batch limit). -}
exchange :: (MonadLog m, MonadIO m) => CcmT m (Seq (NodeId, ByteString))
exchange = do
  posts <- tryRecv
  sendLimit
  return posts

messagesToRecv :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
messagesToRecv = do
  bsm <- view _2
  return $ isEmptyInbox bsm

messagesToSend :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
messagesToSend = do
  -- Does every peerFrame exceed our own PostStore entry?
  -- Are the requests empty?
  undefined

{- | 'STM' test that returns 'True' if there there is material to
   exchange (send or receive) with peers.

   This will always be 'True' immediately after using 'publish'.
-}
readyForExchange :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
readyForExchange = do
  r <- messagesToRecv
  s <- messagesToSend
  return $ (||) <$> r <*> s

{- | Publish a new causal-ordered post, which is dependent on all posts meeting one of the following criteria:

   1. Any post that has been previously published by this node.

   2. Any post that has been returned by calls to 'exchange' or 'tryRecv'.
-}
publish :: (MonadLog m, MonadIO m) => ByteString -> CcmT m ()
publish = undefined

runCcm
  :: CcmConfig
  -> NodeId
  -> Map NodeId MyAddr
  -> CcmT (LogIO IO) a
  -> LogIO IO a
runCcm config self addrs comp = do
  bsm <- runBsm self addrs
  evalStateT (runReaderT comp (config,bsm)) newCcmState

getSelf :: (Monad m) => CcmT m NodeId
getSelf = getSelfId . snd <$> ask

getPeers :: (Monad m) => CcmT m (Set NodeId)
getPeers = getPeerIds . snd <$> ask

allPeersReady :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
allPeersReady = undefined

setTransmissionMode :: (Monad m) => TransmissionMode -> CcmT m ()
setTransmissionMode = undefined
