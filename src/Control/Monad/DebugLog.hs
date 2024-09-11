{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.DebugLog
  ( MonadLog (..)
  , Selector
  , DebugAtom (..)
  , parseDebugSelector
  , prettySelector
  , LogIO
  , runLogIO
  , runLogStdout
  , runLogStdoutC
  , runLogTQueue
  , passLogIO
  , forkLogIO
  , passLogIOF
  ) where

import Control.Monad.DebugLog.Selector

import Control.Concurrent (forkIO,ThreadId)
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State (StateT (StateT))
import Control.Monad.Writer (WriterT (WriterT))
import qualified Data.List as List
import Data.Set (Set)
import qualified Data.Set as Set
import Lens.Micro.Platform

class (Monad m) => MonadLog m where
  dlog :: [String] -> String -> m ()
  -- ^ Log a 'String' on the given channel
  dprefix :: [String] -> m a -> m a
  -- ^ Add a prefix to all uses of 'dlog' in the given action

instance (MonadLog m) => MonadLog (ExceptT e m) where
  dlog l s = lift $ dlog l s
  dprefix l (ExceptT m) = ExceptT $ dprefix l m

instance (MonadLog m) => MonadLog (ReaderT r m) where
  dlog l s = lift $ dlog l s
  dprefix l (ReaderT m) = ReaderT $ dprefix l <$> m

instance (MonadLog m) => MonadLog (StateT s m) where
  dlog l s = lift $ dlog l s
  dprefix l (StateT m) = StateT $ dprefix l <$> m

instance (MonadLog m, Monoid w) => MonadLog (WriterT w m) where
  dlog l s = lift $ dlog l s
  dprefix l (WriterT m) = WriterT $ dprefix l m

data LogEnv
  = LogEnv
    { _leActivePrefixes :: Set Selector
    , _leAction :: String -> IO ()
    , _lePrefix :: [String]
    }

makeLenses ''LogEnv

-- | A 'MonadLog' that applies an 'IO' action to logs within a given
-- level.
newtype LogIO m a
  = LogIO
    { runLogIO' :: ReaderT LogEnv m a
    }

instance (Functor m) => Functor (LogIO m) where
  fmap f (LogIO m) = LogIO (fmap f m)

instance (Applicative m) => Applicative (LogIO m) where
  pure = LogIO . pure
  LogIO f <*> LogIO v = LogIO (f <*> v)

instance (Monad m) => Monad (LogIO m) where
  LogIO m >>= f = LogIO (m >>= (\a -> runLogIO' $ f a))

instance MonadTrans LogIO where
  lift m = LogIO (lift m)

instance (MonadIO m) => MonadIO (LogIO m) where
  liftIO m = LogIO (liftIO m)

instance (MonadError e m) => MonadError e (LogIO m) where
  throwError = lift . throwError
  catchError (LogIO m) f = LogIO $
    catchError m (\e -> runLogIO' $ f e)

anyMatch l ls = not.null $ Set.filter (`selectorMatch` l) ls

instance (MonadIO m) => MonadLog (LogIO m) where
  dlog l s = LogIO $ do
    e <- ask
    let result = anyMatch ((e^.lePrefix) ++ l) (e^.leActivePrefixes)
    if result
      then liftIO $ (e^.leAction) (show l ++ " " ++ s)
      else return ()
  dprefix l (LogIO m) = LogIO (withReaderT (lePrefix %~ (++ l)) m)

runNoLog :: LogIO m a -> m a
runNoLog m = runLogIO m Set.empty (\_ -> return ())

runLogIO :: LogIO m a -> Set Selector -> (String -> IO ()) -> m a
runLogIO m l a = runLogIO'' m (LogEnv l a [])

-- | Run a 'LogIO' for the given level, action, and prefix.
runLogIO'' :: LogIO m a -> LogEnv -> m a
runLogIO'' (LogIO m) = runReaderT m

-- | Run a 'LogIO' that prints logs to stdout. The provided 'String'
-- is used as a prefix.
runLogStdout :: LogIO m a -> Set Selector -> m a
runLogStdout (LogIO m) l =
  let action s = putStrLn s
  in runReaderT m (LogEnv l action [])

-- | Run a 'LogIO' that feeds logs into a queue, automatically
-- spawning a thread that will continually print them until the
-- process terminates.  Essentially, this is a thread-safe version of
-- 'runLogStdout'.
runLogStdoutC :: (MonadIO m) => LogIO m a -> Set Selector -> m a
runLogStdoutC (LogIO m) l = do
  queue <- liftIO newTQueueIO
  let
    action s = atomically $ writeTQueue queue s
  liftIO . forkIO . forever $ do
    log <- atomically . readTQueue $ queue
    putStrLn log
  runReaderT m (LogEnv l action [])

-- | Run a 'LogIO' that writes logs to a 'TQueue'.
runLogTQueue :: LogIO m a -> Set Selector -> TQueue String -> m a
runLogTQueue (LogIO m) l queue =
  let action s = atomically $ writeTQueue queue s
  in runReaderT m (LogEnv l action [])

-- | Transform a @'LogIO' l 'IO'@ action into an 'IO' action that has
-- the same behavior.  This is useful when you need to fork off a new
-- thread that should use the same logging system, for example.
passLogIO :: (MonadIO m) => LogIO m a -> LogIO m (m a)
passLogIO (LogIO m) = LogIO $ do
  e <- ask
  return (runLogIO'' (LogIO m) e)

forkLogIO :: LogIO IO () -> LogIO IO ThreadId
forkLogIO m = liftIO.forkIO =<< passLogIO m

passLogIOF :: (MonadIO m) => (a -> LogIO m b) -> LogIO m (a -> m b)
passLogIOF f = LogIO $ do
  e <- ask
  return $ \a -> runLogIO'' (f a) e

debugNone :: Set [String]
debugNone = Set.empty
