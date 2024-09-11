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
import System.Random

data CcmConfig
  = CcmConfig
    { _cccTransmissionBatch :: PostCount
    , _cccCacheMode :: CacheMode
    , _cccPersistMode :: Bool
    }
  deriving (Show,Eq,Ord)

makeLenses ''CcmConfig

defaultCcmConfig :: CcmConfig
defaultCcmConfig = CcmConfig
  { _cccTransmissionBatch = 10
  , _cccCacheMode = CacheTemp
  , _cccPersistMode = True
  }

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
      -- ^ The clock of posts that have been received or published locally.
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
    , _transmissionMode :: TransmissionMode
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
  , _transmissionMode = TMNormal
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

{- | Get a post sequence from the store by its sender Id and sequence
   number.  If the starting post is not there, you get an error! -}
postRange :: NodeId -> SeqNum -> Maybe PostCount -> SimpleGetter CcmState (Seq Post)
postRange i sn mpc = to $ \s ->
  let
    (count,posts) = s ^. postHistory i
    appendix = Seq.drop (fromIntegral sn - fromIntegral count) posts
  in case mpc of
    Just pc -> Seq.take (fromIntegral pc) appendix
    Nothing -> appendix

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

openFrames :: (Monad m) => CcmT m (Map NodeId SeqNum)
openFrames = do
  self <- getSelf
  snNext <- nextNum self <$> use inputClock
  frames <- use ccmPeerFrames
  return $ Map.filter (\s -> s < snNext) frames

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
    -- The sender might be the original creator of the post, or the
    -- sender might be retransmitting it.
    let creator = p^.postCreator
    pc <- use $ received creator
    if
      | p^.seqNum == pc -> do
        -- Deliver into store
        postHistory creator . _2 %= (Seq.|> p)
        -- Input into sorter
        output <- lift $ zoom sortState (Sort.sortRemote creator (p^.postDeps))
        -- Each output is a NodeId referring to the next un-output
        -- post in the post store.
        for_ output $ \(i,sn) -> do
          p <- use $ postById i sn
          sortOutput %= (Seq.|> (p^.postCreator, p^.postContent))

      | p^.seqNum > pc -> do
        -- Send @Backup pc@ command to sender
        sendMsgMode sender (Backup pc)

      | p^.seqNum < pc -> do
        -- Nothing to do, we already have the message
        return ()
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
sendLimit' mpc = do
  self <- getSelf
  peers <- getPeers
  -- Send messages for each frame that is not maxed out.
  for_ peers $ \i -> do
    snF <- use $ peerFrame i
    snNext <- nextNum self <$> use inputClock
    if snF < snNext
      then do
        posts <- use $ postRange self snF mpc
        -- Advance frame
        peerFrame i += fromIntegral (length posts)
        -- Bsm-send each post, according to transmission configuration
        for_ posts $ \p -> sendMsgMode i (PostMsg p)
      else
        return ()
  -- Send for each open retransmission request.
  rs <- use openRequests
  for_ rs $ \i -> undefined

sendMsgMode :: (MonadLog m, MonadIO m) => NodeId -> CcmMsg -> CcmT m ()
sendMsgMode i m = do
  tmode <- use transmissionMode
  case tmode of
    TMLossy d -> do
      -- Randomness based on @d@ as a probability... take a random
      -- 'Double' on the interval [0,1], and send the message if it
      -- matches or falls below @d@.
      result <- (<= d) <$> randomRIO (0,1)
      if result
        then actuallySendMsg i m
        else return ()
    TMSubNetwork (SendTo s) | not $ Set.member i s -> return ()
    _ -> actuallySendMsg i m

actuallySendMsg :: (MonadLog m, MonadIO m) => NodeId -> CcmMsg -> CcmT m ()
actuallySendMsg i m = do
  bsm <- view $ _2
  liftIO . atomically $
    sendBsm bsm (SendTo $ Set.singleton i) (Store.encode m)

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

messagesToSend :: (MonadLog m, MonadIO m) => CcmT m Bool
messagesToSend = do
  -- Does every peerFrame exceed our own PostStore entry?
  fsEmpty <- null <$> openFrames
  -- Are the requests empty?
  rsEmpty <- null <$> use openRequests
  return $ not (fsEmpty && rsEmpty)

{- | 'STM' test that returns 'True' if there there is material to
   exchange (send or receive) with peers.

   This will always be 'True' immediately after using 'publish'.
-}
readyForExchange :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
readyForExchange = do
  r <- messagesToRecv
  s <- messagesToSend
  return $ (||) <$> r <*> pure s

{- | Publish a new causal-ordered post, which is dependent on all posts
   that meet one of the following criteria:

   1. Any post that has been published by this node.

   2. Any post that has been returned by calls to 'exchange' or 'tryRecv'.

   Note that 'publish' does not directly send any messages.  Future
   calls to 'exchange' will transmit the new post to the network.
-}
publish :: (MonadLog m, MonadIO m) => ByteString -> CcmT m ()
publish bs = do
  self <- getSelf
  -- Get dependency clock for new post
  c <- lift . zoom sortState $ Sort.sortLocal self
  let
    post = Post
      { _postCreator = self
      , _postDeps = c
      , _postContent = bs
      }
  -- Add post to store
  postHistory self . _2 %= (Seq.|> post)
  -- Tick input clock
  inputClock %= tick self
  -- Tick known clock
  knownClock %= tick self

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
allPeersReady = do
  bsm <- view _2
  return (allReady bsm)

setTransmissionMode :: (Monad m) => TransmissionMode -> CcmT m ()
setTransmissionMode t = transmissionMode .= t
