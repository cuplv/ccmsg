{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Sort
  ( State
  , newState
  , sortLocal
  , sortRemote
  , getOutputClock
  ) where

import Prelude hiding (sort)

import Network.Ccm.Lens
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Monad.State hiding (State)
import Data.Foldable (for_)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq

{- | A reference to a post. -}
data Ref
  = Ref
    { _sender :: NodeId -- ^ Node that made the post
    , _clock :: VClock -- ^. Causal dependencies
    }
  deriving (Show,Eq,Ord)

makeLenses ''Ref

{- | Make a 'Ref' from the 'NodeId' that created the post and the 'VClock'
   recording the post's dependencies. -}
mkRef :: NodeId -> VClock -> Ref
mkRef = Ref

seqNum :: SimpleGetter Ref SeqNum
seqNum = to $ \r -> nextNum (r^.sender) (r^.clock)

msgId :: SimpleGetter Ref (NodeId, SeqNum)
msgId = to $ \r -> (r^.sender, r^.seqNum)

{- | A queue of post 'Refs' waiting until a particular post has been
   output. -}
data Block
  = Block
    { _bSeqNum :: SeqNum'
    , _bRefs :: Seq Ref
    }
  deriving (Eq)

makeLenses ''Block

emptyBlock :: Block
emptyBlock = Block
  { _bSeqNum = snInf
  , _bRefs = Seq.Empty
  }

{- | The state of the causal sorting machine. -}
data State
  = State
    { _blockMap :: Map NodeId Block
    , _outputClock :: VClock
    , _outputBuffer :: Seq (NodeId,SeqNum)
    }

makeLenses ''State

{- | An initial empty state for the causal sorting machine. -}
newState :: State
newState = State
  { _blockMap = Map.empty
  , _outputClock = zeroClock
  , _outputBuffer = Seq.Empty
  }

type SortT = StateT State

blocks :: NodeId -> Lens' State Block
blocks i = blockMap . at i . non emptyBlock

-- Algorithm

{- | Input a new post from the local node, which is automatically ready
   for output, returning the 'VClock' representing its dependencies. -}
sortLocal
  :: (Monad m)
  => NodeId
  -> StateT State m VClock
sortLocal i =
  -- Modify clock to include local post, and return the original
  -- clock value.
  outputClock <<%= tick i

{- | Input the 'NodeId' sender and 'VClock' dependencies of a new post
   received from the network, returning a sequence of reference to
   output posts that can now be passed to the application in causal
   order.

   If any causal dependencies of the new post have not yet been
   received, 'Seq.Empty' will be returned.
-}
sortRemote
  :: (Monad m)
  => NodeId
  -> VClock
  -> StateT State m (Seq (NodeId,SeqNum))
sortRemote i c = do
  let r = mkRef i c
  -- This function should always begin and end with an empty
  -- 'outputBuffer' buffer.
  result <- tryOutput r
  if result
    then do
      -- Try outputting any refs that were waiting on the new post,
      -- and any that were waiting on them, and so on.
      sortRec r
      -- Return outputs and clear output buffer
      outputBuffer <<.= Seq.Empty
    else
      return Seq.Empty

sortRec
  :: (Monad m)
  => Ref
  -> SortT m ()
sortRec r = do
  -- Get any refs that were waiting on the given ref.
  revs <- revive r
  -- Try to output them, collecting refs that were output.  This has
  -- the effect of re-deferring non-output refs, and adding output
  -- refs to the 'outputBuffer' buffer.
  outputs <- filterMSeq tryOutput revs
  -- Recurse on newly-output refs.
  for_ outputs sortRec

tryOutput :: (Monad m) => Ref -> SortT m Bool
tryOutput r = do
  oc <- use outputClock
  -- Check if r's clock is subsumed by the current output clock,
  -- representing all posts that have already been output.
  case leVC' (r^.clock) oc of
    Right () -> do
      -- If so, add the post to the output clock and the output
      -- buffer.
      outputClock %= tick (r^.sender)
      outputBuffer %= (Seq.|> (r^.msgId))
      return True
    Left x -> do
      -- If not, defer the post until the given remote post (x) has
      -- been output.
      defer r x
      return False

defer :: (Monad m) => Ref -> (NodeId, SeqNum) -> SortT m ()
defer r (i,sn) = do
  let sn1 = snFin sn
  sn2 <- use $ blocks i . bSeqNum
  if
    -- When our deferred post is waiting on an earlier post than was
    -- waited on before, we put it at the beginning of the queue.
    | sn1 `infLT` sn2 -> do
      blocks i . bSeqNum .= sn1
      blocks i . bRefs %= (r Seq.<|)
    -- Otherwise, we put it at the end, and leave the blocking sn
    -- unchanged.
    | otherwise -> do
      blocks i . bRefs %= (Seq.|> r)

{- | Remove from blocks any refs that were waiting on the given post ID,
   returning them. -}
revive :: (Monad m) => Ref -> SortT m (Seq Ref)
revive r = do
  let
    i = r^.sender
    sn1 = snFin (r^.seqNum)
  sn2 <- use $ blocks i . bSeqNum
  if
    | sn2 == snInf -> return Seq.Empty
    | sn2 `infLE` sn1 -> do
      blocks i . bSeqNum .= snInf
      blocks i . bRefs <<.= Seq.Empty
    | otherwise -> return Seq.Empty

filterMSeq :: (Monad m) => (a -> m Bool) -> Seq a -> m (Seq a)
filterMSeq test s = do
  case s of
    Seq.Empty -> pure Seq.Empty
    a Seq.:<| s -> do
      result <- test a
      if result
        then (a Seq.:<|) <$> filterMSeq test s
        else filterMSeq test s

{- | Get the current output clock. -}
getOutputClock :: (Monad m) => SortT m VClock
getOutputClock = use outputClock
