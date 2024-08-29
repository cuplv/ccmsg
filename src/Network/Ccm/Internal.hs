{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Internal where

import Network.Ccm.Bsm
import Network.Ccm.Lens
import qualified Network.Ccm.Sort as Sort
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Monad.Reader
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

-- | A causal messaging post
data Post
  = Post
    { _postSender :: NodeId
    , _postDeps :: VClock
    , _postContent :: ByteString
    }
  deriving (Show,Eq,Ord)

makeLenses ''Post

seqNum :: SimpleGetter Post SeqNum
seqNum = to $ \r -> nextNum (r^.postSender) (r^.postDeps)

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

-- tryRecv :: MonadIO m => CcmT m (Seq ByteString)
