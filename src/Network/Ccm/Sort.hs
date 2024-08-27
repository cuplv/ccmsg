{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.Sort where

import Network.Ccm.Lens
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Sequence (Seq,(<|),(|>))
import qualified Data.Sequence as Seq

-- Nothing represents infinity
type SeqNum' = Maybe SeqNum

infLE :: SeqNum' -> SeqNum' -> Bool
infLE (Just n1) (Just n2) = n1 <= n2
infLE _ Nothing = True
infLE _ _ = False

infLT :: SeqNum' -> SeqNum' -> Bool
infLT n1 n2 = infLE n1 n2 && (n1 /= n2)

snFin :: SeqNum -> SeqNum'
snFin = Just

snInf :: SeqNum'
snInf = Nothing

data Ref
  = Ref
    { _sender :: NodeId
    , _clock :: VClock
    }
  deriving (Eq)

makeLenses ''Ref

seqNum :: SimpleGetter Ref SeqNum
seqNum = to $ \r -> nextNum (r^.sender) (r^.clock)

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

data SortState
  = SortState
    { _blockMap :: Map NodeId Block
    , _outputClock :: VClock
    , _currentOutput :: Seq NodeId
    }

makeLenses ''SortState

newSortState :: SortState
newSortState = SortState
  { _blockMap = Map.empty
  , _outputClock = zeroClock
  , _currentOutput = Seq.Empty
  }

type Sort = StateT SortState

blocks :: NodeId -> Lens' SortState Block
blocks i = blockMap . at i . non emptyBlock

data ClockResult
  = Output
  | Defer (NodeId, SeqNum)

punchClock :: (Monad m) => Ref -> Sort m ClockResult
punchClock r = do
  oc <- use outputClock
  case leVC' (r^.clock) oc of
    Right () -> do
      outputClock %= tick (r^.sender)
      return Output
    Left x -> return $ Defer x

defer :: (Monad m) => Ref -> (NodeId, SeqNum) -> Sort m ()
defer r (i,sn) = do
  let sn1 = snFin sn
  sn2 <- use $ blocks i . bSeqNum
  if
    -- When our deferred post is waiting on an earlier post than was
    -- waited on before, we put it at the beginning of the queue.
    | sn1 `infLT` sn2 -> do
      blocks i . bSeqNum .= sn1
      blocks i . bRefs %= (r <|)
    -- Otherwise, we put it at the end, and leave the blocking sn
    -- unchanged.
    | otherwise -> do
      blocks i . bRefs %= (|> r)

revive :: (Monad m) => (NodeId, SeqNum) -> Sort m (Seq Ref)
revive (i,sn) = do
  let sn1 = snFin sn
  sn2 <- use $ blocks i . bSeqNum
  if
    | sn2 == snInf -> return Seq.Empty
    | sn1 `infLE` sn2 -> do
      blocks i . bSeqNum .= snInf
      blocks i . bRefs <<.= Seq.Empty
    | otherwise -> return Seq.Empty
