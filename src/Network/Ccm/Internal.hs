{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Internal where

import Control.Monad.DebugLog
import Network.Ccm.Bsm
import Network.Ccm.Lens
import qualified Network.Ccm.Sort as Sort
import Network.Ccm.Switch
import Network.Ccm.Timer
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Concurrent (killThread)
import Control.Concurrent.STM
import Control.Monad (foldM,when)
import Control.Monad.Reader
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (catMaybes)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Store.TH (makeStore)
import qualified Data.Store as Store
import Data.Traversable (for)
import System.Random

data CcmConfig
  = CcmConfig
    { _cccTransmissionBatch :: PostCount
    , _cccCacheMode :: CacheMode
    , _cccPersistMode :: Bool
    , _cccHeartbeatMicros :: Int
    , _cccRetransMicros :: Int
    }
  deriving (Show,Eq,Ord)

makeLenses ''CcmConfig

defaultCcmConfig :: CcmConfig
defaultCcmConfig = CcmConfig
  { _cccTransmissionBatch = 10
  , _cccCacheMode = CacheTemp
  , _cccPersistMode = True
  , _cccHeartbeatMicros = defaultHeartbeatMicros
  , _cccRetransMicros = defaultRetransMicros
  }

{-| By default, a heartbeat message is sent every 10ms. -}
defaultHeartbeatMicros :: Int
defaultHeartbeatMicros = 10000

{-| By default, a retrans request is sent every 200ms. -}
defaultRetransMicros :: Int
defaultRetransMicros = 200000

-- | A causal messaging post
data Post
  = Post
    { _postCreator :: NodeId
    , _postDeps :: VClock
    , _postContent :: ByteString
    }
  deriving (Eq,Ord)

makeLenses ''Post
makeStore ''Post

instance Show Post where
  show p =
    "Post("
    ++ show (p^.postCreator)
    ++ ", "
    ++ show (p^.postDeps)
    ++ ")"

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
  | Heartbeat VClock
    -- ^ Notification that the sender has received (and output) the
    -- contents of the given clock.
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
      -- in dependencies and heartbeats.
    , _ccmPeerClocks :: Map NodeId VClock
      -- ^ The clocks reported as received (and output) by peers.
    , _ccmPeerFrames :: Map NodeId SeqNum
      -- ^ The next local post that should be sent to each peer.
    , _ccmPeerRequests :: Map (NodeId, NodeId) (SeqNum, SeqNum)
      -- ^ @(i1,i2) -> (sn1,sn2)@ means that we should send @i2@'s posts
      -- with sequence numbers starting at @sn1@ and ending at @sn2 - 1@.
    , _transmissionConfig :: TransmissionConfig
    , _heartbeatTimerSwitch :: Switch
    , _retransTimers :: Map NodeId (Timer, SeqNum)
    }

makeLenses ''CcmState

newCcmState :: Switch -> CcmState
newCcmState sw = CcmState
  { _sortState = Sort.newState
  , _sortOutput = Seq.Empty
  , _ccmPostStore = Map.empty
  , _inputClock = zeroClock
  , _knownClock = zeroClock
  , _ccmPeerClocks = Map.empty
  , _ccmPeerFrames = Map.empty
  , _ccmPeerRequests = Map.empty
  , _transmissionConfig = defaultTransmissionConfig
  , _heartbeatTimerSwitch = sw
  , _retransTimers = Map.empty
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

storedPostRange :: NodeId -> SimpleGetter CcmState (SeqNum, PostCount)
storedPostRange i = to $ \s ->
  let (count,posts) = s ^. postHistory i
  in (count, fromIntegral (Seq.length posts))

received :: NodeId -> SimpleGetter CcmState PostCount
-- -- This version might have been causing errors, even though it
-- -- looks safe...
--
-- received i = to $ \s ->
--   let (start,length) = s ^. storedPostRange i
--   in start + length
received i = to $ \s -> nextNum i (s^.inputClock)

peerFrame :: NodeId -> Lens' CcmState SeqNum
peerFrame i = ccmPeerFrames . at i . non 0

peerRequest :: NodeId -> NodeId -> Lens' CcmState (Maybe (SeqNum,SeqNum))
peerRequest i1 i2 = ccmPeerRequests . at (i1,i2)

openRequests :: SimpleGetter CcmState (Set (NodeId, NodeId))
openRequests = to $ \s -> Map.keysSet (s ^. ccmPeerRequests)

getSelfNext :: (Monad m) => CcmT m SeqNum
getSelfNext = do
  self <- getSelf
  nextNum self <$> use inputClock

openFrames :: (Monad m) => CcmT m (Map NodeId SeqNum)
openFrames = do
  peers <- getPeers
  frames <- for (Set.toList peers) $ \i -> do
    sn <- use $ peerFrame i
    return (i, sn)
  snNext <- getSelfNext
  return $ Map.filter (\sn -> sn < snNext) (Map.fromList frames)

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
  for_ msgs $ \m ->
    dlog ["ccm","comm"] $
      "Received message: "
      ++ show m
  -- Then handle each message in turn, collecting any causal-ordered
  -- posts that can be delivered to the application.
  for_ msgs (uncurry handleCcmMsg)

  when (not $ null msgs) $ do
    -- Update retrans timers according to new clock values
    setRetrans
    -- Drop any stored posts that are now garbage
    collectGarbage

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
    let
      creator = p^.postCreator
      creatorClock = tick creator (p^.postDeps)

    -- Update knowledge of existence of posts
    peerClock creator %= joinVC creatorClock
    knownClock %= joinVC creatorClock

    pc <- use $ received creator
    if
      | p^.seqNum == pc -> do
        -- Deliver into store
        postHistory creator . _2 %= (Seq.|> p)
        -- Input into sorter
        inputClock %= tick creator
        output <- lift . zoom sortState $
          Sort.sortRemote creator (p^.postDeps)
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
    dlog ["backup"] $
      "Backed up to "
      ++ show sn
      ++ " for node "
      ++ show sender
    peerFrame sender .= sn
  Retrans i sn -> do
    -- Check post store range for @i@.  We must have @sn@, and we will
    -- send up to the end of the store range.
    (start, length) <- use $ storedPostRange i
    if start <= sn && sn < (start + length)
      then peerRequest sender i .= Just (sn, start + length)
      else return ()
  Heartbeat c -> do
    peerClock sender %= joinVC c
    knownClock %= joinVC c
    rc <- use $ received sender
    let next = nextNum sender c
    if next > rc
      then sendMsgMode sender (Backup rc)
      else return ()

sendLimit' :: (MonadLog m, MonadIO m) => Maybe PostCount -> CcmT m ()
sendLimit' mpc = do
  self <- getSelf
  peers <- getPeers
  -- Send messages for each frame that is not maxed out.
  for_ peers $ \i -> do
    snF <- use $ peerFrame i
    snNext <- getSelfNext
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
  rs <- use ccmPeerRequests
  rs' <- flip Map.traverseMaybeWithKey rs $ \(i1,i2) (sn1,sn2) -> do
    let
      n = case mpc of
        Just count -> min count (sn2 - sn1)
        Nothing -> sn2 - sn1
    posts <- use $ postRange i2 sn1 (Just n)
    -- Send each post
    for_ posts $ \p -> sendMsgMode i1 (PostMsg p)
    -- Advance the request window
    let sn1' = sn1 + fromIntegral (length posts)
    dlog ["retrans"] $
      "Sent "
      ++ show (length posts)
      ++ " retrans posts for "
      ++ show (i1,i2,sn1)
    -- If the window size is > 0, keep it
    if sn1' < sn2
      then return $ Just (sn1',sn2)
      else return Nothing
  ccmPeerRequests .= rs'

sendMsgMode :: (MonadLog m, MonadIO m) => NodeId -> CcmMsg -> CcmT m ()
sendMsgMode i m = do
  dlog ["ccm","comm"] $
    "Send to "
    ++ show i
    ++ " msg "
    ++ show m
  tmc <- use transmissionConfig
  case tmc^.tmLinks of
    Just (SendTo s) | not $ Set.member i s ->
      dlog ["ccm","comm"] $ "Simulated transmission failure: link disabled"
    _ -> case tmc^.tmLossy of
      Just d -> do
        -- Randomness based on @d@ as a probability... take a random
        -- 'Double' on the interval [0,1], and send the message if it
        -- matches or falls below @d@.
        result <- (<= d) <$> randomRIO (0,1)
        if result
          then
            actuallySendMsg i m
          else
            dlog ["ccm","comm"] $ "Simulated transmission failure: random drop"
      Nothing ->
        actuallySendMsg i m

broadcastMode :: (MonadLog m, MonadIO m) => CcmMsg -> CcmT m ()
broadcastMode m = do
  peers <- getPeers
  for_ peers $ \i -> sendMsgMode i m

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

sendHeartbeat :: (MonadLog m, MonadIO m) => CcmT m ()
sendHeartbeat = do
  oc <- use $ sortState . Sort.getOutputClock
  dlog ["heartbeat"] $ "Sending heartbeat " ++ show oc
  let m = Heartbeat oc
  peers <- getPeers
  for_ peers $ \i -> sendMsgMode i m

data ETask
  = EHeartbeat
  | ERetransRequest
  deriving (Show,Eq,Ord)

data Exchange
  = Exchange [ETask]
  deriving (Show,Eq,Ord)

{- | The minimal exchange command, which will receive and send any ready
   messages. -}
eRecvSend :: Exchange
eRecvSend = Exchange []

{- | Communicate with peers.

   This will handle any messages that have been received, and will
   send any waiting messages (up to the transmission batch limit). -}
exchange :: (MonadLog m, MonadIO m) => Exchange -> CcmT m (Seq (NodeId, ByteString))
exchange (Exchange tasks) = do
  for_ tasks $ \t -> case t of
    EHeartbeat -> sendHeartbeat
    ERetransRequest -> do
      reqs <- collectTimers
      for_ reqs $ \(i,sn) -> do
        dlog ["retrans"] $
          "Sending retrans request: "
          ++ show (i,sn)
        broadcastMode $ Retrans i sn
      
  posts <- tryRecv
  sendLimit
  fs <- use ccmPeerFrames
  return posts

messagesToRecv :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
messagesToRecv = do
  bsm <- view _2
  return $ not <$> isEmptyInbox bsm

messagesToSend :: (MonadLog m, MonadIO m) => CcmT m Bool
messagesToSend = do
  -- Does every peerFrame exceed our own PostStore entry?
  fsEmpty <- null <$> openFrames
  -- Are the requests empty?
  rsEmpty <- null <$> use openRequests
  let r = not fsEmpty || not rsEmpty
  return r

{- | Returns an 'STM' action that blocks until any timer expires, and
   then returns that timer.  This does not remove the timer from the
   'retransTimers' state. -}
awaitTimers :: (MonadLog m, MonadIO m) => CcmT m (STM (NodeId, SeqNum))
awaitTimers = do
  ts <- use retransTimers
  let
    f [] = retry
    f ((i,(a,sn)):as) =
      (awaitTimer a >> return (i,sn)) `orElse` f as
  return $ f (Map.toList ts)

newRetransTimer :: (MonadLog m, MonadIO m) => CcmT m Timer
newRetransTimer = do
  micros <- view $ _1 . cccRetransMicros
  forkTimer micros

{- | Check all existing timers, restarting expired ones and returning
   their details. -}
collectTimers :: (MonadLog m, MonadIO m) => CcmT m [(NodeId, SeqNum)]
collectTimers = do
  ts <- use retransTimers
  kc <- use knownClock
  ic <- use inputClock
  ms <- for (Map.toList ts) $ \(i,(t,sn)) -> do
    let
      iNext = nextNum i ic
      kNext = nextNum i kc
    done <- liftIO . atomically $ tryTimer t
    if
      -- Error cases
      | iNext < sn ->
        error "collectTimers: inputClock went backwards?"
      | iNext > kNext ->
        error $
          "collectTimers: inputClock greater than knownClock? Input: "
          ++ show ic
          ++ ", Known: "
          ++ show kc

      -- Don't do anything when timer is not done.
      | not done -> return Nothing

      -- Three cases when timer is done:
      --
      -- 1. No progress since timer started, so we send a retrans
      -- request message and restart the timer.
      | done && iNext == sn -> do
        dlog ["retrans"] $
          "No progress, sending retrans and restarting timer for "
          ++ show (i,sn)
        t' <- newRetransTimer
        retransTimers . at i .= Just (t',sn)
        return (Just (i,sn))

      -- 2. Some progress but not complete, so we restart the timer
      -- and bump its SeqNum without sending a message.
      | iNext > sn && iNext < kNext -> do
        dlog ["retrans"] $
          "Some progress, restarting timer for "
          ++ show (i,iNext)
        t' <- newRetransTimer
        retransTimers . at i .= Just (t',iNext)
        return Nothing

      -- 3. Progress and complete, so we don't start a new timer.
      | iNext == kNext -> do
        dlog ["retrans"] $
          "Up to date, removing timer for "
          ++ show i
        retransTimers . at i .= Nothing
        return Nothing
  return $ catMaybes ms

{- | Set a new retrans timer for the given 'NodeId' and 'SeqNum', unless
   a timer is already set. -}
startRetransReq :: (MonadLog m, MonadIO m) => NodeId -> SeqNum -> CcmT m ()
startRetransReq i sn2 = do
  r <- use $ retransTimers . at i
  let
    setNew = do
      t2 <- newRetransTimer
      retransTimers . at i .= Just (t2,sn2)      
  case r of
    Nothing -> do
      dlog ["retrans"] $
        "Started a new retrans timer for " ++ show (i,sn2)
      setNew
    _ -> return ()

{- | Set retrans timers as necessary, according to 'knownClock' and
   'inputClock' -}
setRetrans :: (MonadLog m, MonadIO m) => CcmT m ()
setRetrans = do
  peers <- getPeers
  kc <- use knownClock
  ic <- use inputClock
  for_ peers $ \i -> do
    let
      kNext = nextNum i kc
      iNext = nextNum i ic
    if kNext > iNext
      then startRetransReq i iNext
      else return ()

{- | 'STM' action that blocks until there is material to exchange (send
   or receive) with peers.

   This will always return immediately, without retrying, the first
   time it is called after 'publish'.
-}
awaitExchange :: (MonadLog m, MonadIO m) => CcmT m (STM Exchange)
awaitExchange = do
  sw <- use heartbeatTimerSwitch
  r <- messagesToRecv
  s <- messagesToSend
  ats <- awaitTimers
  let
    -- Check whether a heartbeat message should be sent.
    hAction = do
      -- The 'take' will block until the heartbeat timer expires.
      takeSwitch sw
      -- The 'pass' will immediately restart the heartbeat timer.
      passSwitch sw
      -- Record that a heartbeat message should be sent.
      return $ Exchange [EHeartbeat]

    -- Check whether any retrans timers have expired
    tAction = do
      ats
      return $ Exchange [ERetransRequest]

    -- Check whether messages are ready to receive.
    rAction = do
      isR <- r
      check isR -- retries if 'False'
      return (Exchange [])

    -- Check whether messages are ready to send.
    sAction = if s
      then return (Exchange [])
      else retry
  return $ rAction `orElse` sAction `orElse` tAction `orElse` hAction

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
  (tid,sw) <- forkTimerSwitch (config^.cccHeartbeatMicros)
  liftIO.atomically $ passSwitch sw
  a <- evalStateT (runReaderT comp (config,bsm)) (newCcmState sw)
  liftIO $ killThread tid
  return a

getSelf :: (Monad m) => CcmT m NodeId
getSelf = getSelfId . snd <$> ask

getPeers :: (Monad m) => CcmT m (Set NodeId)
getPeers = getPeerIds . snd <$> ask

getAllNodes :: (Monad m) => CcmT m (Set NodeId)
getAllNodes = do
  self <- getSelf
  peers <- getPeers
  return (Set.insert self peers)

allPeersReady :: (MonadLog m, MonadIO m) => CcmT m (STM Bool)
allPeersReady = do
  bsm <- view _2
  return (allReady bsm)

{- | Returns true when all peers have reported receiving (and causally
   outputting) all posts from the local node. -}
allPeersUpToDate :: (MonadLog m, MonadIO m) => CcmT m Bool
allPeersUpToDate = do
  self <- getSelf
  ps <- Set.toList <$> getPeers
  oc <- use (sortState . Sort.getOutputClock)
  case lookupVC self oc of
    Just sn -> do
      rs <- for ps $ \i -> do
        iClock <- use $ peerClock i
        return $ hasSeen self sn iClock
      return $ and rs
    Nothing -> return True

getOutputPostClock :: (Monad m) => CcmT m VClock
getOutputPostClock = use $ sortState . Sort.getOutputClock

getInputPostClock :: (Monad m) => CcmT m VClock
getInputPostClock = use $ inputClock

getKnownPostClock :: (Monad m) => CcmT m VClock
getKnownPostClock = use $ knownClock

commonClock :: (Monad m) => CcmT m VClock
commonClock = do
  -- pcs <- Map.elems <$> use ccmPeerClocks
  peers <- Set.toList <$> getPeers
  pcs <- for peers $ \i -> use (peerClock i)
  oc <- use $ sortState . Sort.getOutputClock
  let
    -- The base case is the local outputClock
    f [] = oc
    f (c:cs) = c `meetVC` f cs
  return $ f pcs

{- | Drop posts from the store that every node has output. -}
collectGarbage :: (MonadLog m) => CcmT m ()
collectGarbage = do
  all <- getAllNodes
  mc <- commonClock
  dlog ["garbage"] $
    "Meet clock is "
    ++ show mc
  for_ all $ \i -> do
    pc <- use $ postHistory i . _1
    dlog ["garbage"] $
      "So far we have dropped "
      ++ show pc
      ++ " posts from store for "
      ++ show i
    let
      n = nextNum i mc
      -- The number of posts we should drop is the number of posts
      -- everyone has already seen, minus the number of posts we have
      -- already dropped.
      dn = n - pc
    postHistory i . _2 %= Seq.drop (fromIntegral dn)
    pc' <- postHistory i . _1 <%= (+ dn)
    dlog ["garbage"] $
      "Should have dropped "
      ++ show dn
      ++ " posts from store for "
      ++ show i
    -- dlog ["garbage"] $
    --   "Store for "
    --   ++ show i
    --   ++ " now begins at "
    --   ++ show pc'
