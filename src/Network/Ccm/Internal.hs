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
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.Store.TH (makeStore)
import qualified Data.Store as Store

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

-- | A communication manager for a peer node
data Liason

data CcmState
  = CcmState
    { _sortState :: Sort.State
    , _ccmPostStore :: Map NodeId (PostCount, Seq Post)
    , _inputClock :: VClock
    , _knownClock :: VClock
    , _ccmPeerClocks :: Map NodeId VClock
    }

makeLenses ''CcmState

newCcmState :: CcmState
newCcmState = CcmState
  { _sortState = Sort.newState
  , _ccmPostStore = Map.empty
  , _inputClock = zeroClock
  , _knownClock = zeroClock
  , _ccmPeerClocks = Map.empty
  }

peerClock :: NodeId -> Lens' CcmState VClock
peerClock i = ccmPeerClocks . at i . non zeroClock

postHistory :: NodeId -> Lens' CcmState (PostCount, Seq Post)
postHistory i = ccmPostStore . at i . non (0, Seq.Empty)

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

type CcmT m = ReaderT Bsm (StateT CcmState m)

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

tryRecv :: (MonadLog m, MonadIO m) => CcmT m (Seq ByteString)
tryRecv = do
  msgs <- tryReadMsgs
  -- Then handle each message in turn, collecting any causal-ordered
  -- posts that can be delivered to the application.
  --
  -- Each message is either a post that (if in sender-order) can go
  -- into the store and into the sorting machine, or it's a retransmit
  -- / backup request that will modify the per-peer send triggers.
  undefined

tryReadMsgs :: (MonadLog m, MonadIO m) => CcmT m (Seq (NodeId, CcmMsg))
tryReadMsgs = do
  bsm <- ask
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
