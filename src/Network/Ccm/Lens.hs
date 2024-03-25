{-# LANGUAGE RankNTypes #-}

module Network.Ccm.Lens
  ( module Lens.Micro.Platform
  , combineTo
  , nani
  , nonNull
  , nonList
  , nonMap
  , nonCheat
  , nonCheat'
  , maybeModifying
  , eitherModifying
  , (<>=)
  ) where

import Control.Monad.State
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Lens.Micro.Platform

{-| Perform semigroup combination ('<>'). -}
combineTo :: (Monoid a, Monoid s) => ASetter s t a a -> a -> t
combineTo l b = mempty & l <>~ b

{-| Like 'Lens.Micro.Platform.non', but with an explicit check for
  whether a value is the empty value.  This is useful for types
  that do not implement 'Eq', but nonetheless have a clearly
  identifyable "empty" value. -}
nani :: a -> (a -> Bool) -> Lens' (Maybe a) a
nani x pr afb s = f <$> afb (fromMaybe x s)
  where f y = if pr y
                 then Nothing
                 else Just y

{-| A version of 'nani' using 'Data.Monoid.mempty' as the empty value
  and 'Data.Foldable.null' as the empty value test. -}
nonNull :: (Foldable t, Monoid (t a)) => Lens' (Maybe (t a)) (t a)
nonNull = nani mempty null

{-| 'nonNull' specialized to lists, with @[]@ as the empty value. -}
nonList :: Lens' (Maybe [a]) [a]
nonList = nonNull

{-| 'nonNull' specialized to maps, with @Data.Map.empty@ as the empty
  value. -}
nonMap :: (Ord k) => Lens' (Maybe (Map k a)) (Map k a)
nonMap = nonNull

{-| A cheating version of 'nonNull' that has no empty value.  If this
  accesses a 'Nothing' value, it throws a runtime error. -}
nonCheat :: Lens' (Maybe a) a
nonCheat = nani (error "nonCheat got Nothing") (const False)

{-| 'nonCheat' with custom error msg. -}
nonCheat' :: String -> Lens' (Maybe a) a
nonCheat' e = nani (error e) (const False)

{-| Modify state by applying a partial function to a part of the state.
  If the function returns 'Nothing', the state is not modified.  The
  return value indicates whether a modification took place. -}
maybeModifying :: MonadState s m => Lens' s a -> (a -> Maybe a) -> m Bool
maybeModifying l f = do
  a1 <- use l
  case f a1 of
    Just a2 -> l .= a2 >> return True
    Nothing -> return False

{-| Like 'maybeModifying' but for 'Either'.  If a 'Left' value is
  produced, it is returned.  Otherwise, a 'Right' result indicates
  that the modification took place. -}
eitherModifying
  :: MonadState s m
  => Lens' s a
  -> (a -> Either e a)
  -> m (Either e ())
eitherModifying l f = do
  a1 <- use l
  case f a1 of
    Right a2 -> l .= a2 >> return (Right ())
    Left e -> return (Left e)

(<>=) :: (MonadState s m, Semigroup a) => ASetter s s a a -> a -> m ()
l <>= a = l %= (<> a)
