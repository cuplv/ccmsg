{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.VClock
  ( VClock
  , zeroClock
  , tick
  , tickBy
  , nextNum
  , precedes
  , concurrent
  , aggDiff
  , lookupVC
  , joinVC
  , leVC
  , leVC'
  , hasSeen
  , changed
  , activeProcesses
  , toListVC
  , deleteVC
  ) where

import Network.Ccm.Types

import Data.Map (Map)
import qualified Data.Map as Map
import Data.Map.Merge.Lazy
import Data.Store.TH

{- | A vector clock using process ID type @i@. -}
data VClock
  = VClock (Map NodeId SeqNum)
  deriving (Eq,Ord)

instance Show VClock where
  show (VClock m) = "Clock(" ++ show (Map.toList m) ++ ")"

makeStore ''VClock

deleteVC :: NodeId -> VClock -> VClock
deleteVC i (VClock m) = VClock (Map.delete i m)

meetVC :: VClock -> VClock -> VClock
meetVC (VClock m1) (VClock m2) = VClock $ merge
  dropMissing
  dropMissing
  (zipWithMatched $ const min)
  m1
  m2

joinVC :: VClock -> VClock -> VClock
joinVC (VClock m1) (VClock m2) = VClock $ merge
  preserveMissing
  preserveMissing
  (zipWithMatched $ const max)
  m1
  m2

{- | Initial clock, with no observed events for any process. -}
zeroClock :: VClock
zeroClock = VClock Map.empty

{-| Get the sequence number of the latest observed event for the given
  process ID (or 'Nothing' if no events have been observed
  for that process.  Sequence numbers start at @0@.

@
'lookupVC' i ('tick' i 'zeroClock') = 'Just' 0
@
 -}
lookupVC :: NodeId -> VClock -> Maybe SeqNum
lookupVC i (VClock m) = Map.lookup i m

{- | Advance the clock for the given process ID. -}
tick :: NodeId -> VClock -> VClock
tick i (VClock m) = VClock $ Map.alter f i m
  where f (Just n) = Just (n + 1)
        f Nothing = Just 0

{- | Advance the clock for the given process ID, by the given number of
   steps.

@
tickBy 2 "a" ≡ tick "a" . tick "a"
@
 -}
tickBy :: SeqNum -> NodeId -> VClock -> VClock
tickBy 0 _ v = v
tickBy n1 i (VClock m) | n1 > 0 = VClock $ Map.alter f i m
  where f (Just n) = Just (n + n1)
        f Nothing = Just (n1 - 1)

{- | Check that the left clock either precedes or is equal to the right
   clock.  In other words, whether the right clock has seen every event
   that the left clock has seen. -}
leVC :: VClock -> VClock -> Bool
leVC v1@(VClock m1) v2 = and . map f $ Map.keys m1
  where f k = lookupVC k v1 <= lookupVC k v2

{- | Like 'leVC', instead of 'False', it returns @'Left' (i,n)@, where
   @i@ is a node ID for which the left clock has seen more events than
   the right clock, and @n@ is the number of @i@-events the left clock
   has seen. -}
leVC' :: VClock -> VClock -> Either (NodeId,SeqNum) ()
leVC' v1@(VClock m1) v2 = mapM_ f $ Map.toList m1
  where f (k,n1) = case lookupVC k v2 of
          Just n2 | n1 > n2 -> Left (k,n1)
          _ -> Right ()

{- | Check whether one clock denotes an event that happens before
   another.

@
v1 `'precedes'` v1 = 'False'

v1 `'precedes'` ('tick' i v1) = 'True'
@
-}
precedes :: VClock -> VClock -> Bool
precedes v1 v2 = leVC v1 v2 && not (leVC v2 v1)

{- | Check whether two clocks denote concurrent events (or the same
   event). -}
concurrent :: VClock -> VClock -> Bool
concurrent v1 v2 = not (precedes v1 v2) && not (precedes v2 v1)

{-| Get list of all process IDs with at least 1 event. -}
activeProcesses :: VClock -> [NodeId]
activeProcesses (VClock m) = Map.keys m

{-| Get a list of process IDs that have updated between the first clock
  and the second.  The first clock is assumed to precede the second,
  so that all changes are positive.

@
changed v1 (tick "a" . tick "b" $ v1) = ["a","b"]
@
-}
changed :: VClock -> VClock -> [NodeId]
changed v1 v2 = filter
  (\i -> lookupVC i v1 < lookupVC i v2)
  (activeProcesses v2)

toListVC :: VClock -> [(NodeId,SeqNum)]
toListVC (VClock m) = Map.toList m

{-| @('aggDiff' v1 v2)@ gives the positive "aggregate difference"
  between the two clocks.  This is the total number of ticks found in
  @v2@ that are not found in @v1@.

  If @v1@ and @v2@ are concurrent, both @('aggDiff' v1 v2)@ and
  @('aggDiff' v2 v1)@ will be greater than @0@.
-}
aggDiff :: VClock -> VClock -> SeqNum
aggDiff (VClock m1) v2 = Map.foldlWithKey f 0 m1
  where f n i n1 = case lookupVC i v2 of
                     Just n2 | n1 > n2 -> n + (n1 - n2)
                             | otherwise -> n
                     Nothing -> n + n1 + 1

{-| Sequence number of next event from process @i@ that has not yet been
  witnessed by the clock. -}
nextNum :: NodeId -> VClock -> SeqNum
nextNum i v = case lookupVC i v of
  Just n -> n + 1
  Nothing -> 0

{-| @'hasSeen' i n v@ checks whether clock @v@ has seen event @n@ of
  process @i@. -}
hasSeen :: NodeId -> SeqNum -> VClock -> Bool
hasSeen i n v = case lookupVC i v of
  Just n' | n' >= n -> True
  _ -> False
