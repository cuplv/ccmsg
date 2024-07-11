{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

module Control.Monad.DebugLog
  ( MonadLog (..)
  , LogIO
  , runLogIO
  , runLogStdout
  , runLogStdoutC
  , runLogTQueue
  , passLogIO
  , passLogIOF
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State (StateT)
import Control.Monad.Writer (WriterT)

class (Monad m) => MonadLog l m | m -> l where
  dlog :: l -> String -> m ()
  -- ^ Log a 'String' at the given level.

instance (MonadLog l m) => MonadLog l (ExceptT e m) where
  dlog l s = lift $ dlog l s

instance (MonadLog l m) => MonadLog l (ReaderT r m) where
  dlog l s = lift $ dlog l s

instance (MonadLog l m) => MonadLog l (StateT s m) where
  dlog l s = lift $ dlog l s

instance (MonadLog l m, Monoid w) => MonadLog l (WriterT w m) where
  dlog l s = lift $ dlog l s

-- | A 'MonadLog' that applies an 'IO' action to logs within a given
-- level.
newtype LogIO l m a = LogIO { runLogIO' :: ReaderT (l, String -> IO ()) m a }

instance (Functor m) => Functor (LogIO l m) where
  fmap f (LogIO m) = LogIO (fmap f m)

instance (Applicative m) => Applicative (LogIO l m) where
  pure = LogIO . pure
  LogIO f <*> LogIO v = LogIO (f <*> v)

instance (Monad m) => Monad (LogIO l m) where
  LogIO m >>= f = LogIO (m >>= (\a -> runLogIO' $ f a))

instance MonadTrans (LogIO l) where
  lift m = LogIO (lift m)

instance (MonadIO m) => MonadIO (LogIO l m) where
  liftIO m = LogIO (liftIO m)

instance (MonadError e m) => MonadError e (LogIO l m) where
  throwError = lift . throwError
  catchError (LogIO m) f = LogIO $
    catchError m (\e -> runLogIO' $ f e)

instance (MonadIO m, Ord l) => MonadLog l (LogIO l m) where
  dlog l s = LogIO $ do
    (l', action) <- ask
    if l <= l'
      then liftIO $ action s
      else return ()

-- | Run a 'LogIO' for the given level and action.
runLogIO :: LogIO l m a -> l -> (String -> IO ()) -> m a
runLogIO (LogIO m) l f = runReaderT m (l,f)

-- | Run a 'LogIO' that prints logs to stdout. The provided 'String'
-- is used as a prefix.
runLogStdout :: LogIO l m a -> l -> String -> m a
runLogStdout (LogIO m) l prefix =
  let action s = putStrLn $ "[" ++ prefix ++ "] " ++ s
  in runReaderT m (l,action)

-- | Run a 'LogIO' that feeds logs into a queue, automatically
-- spawning a thread that will continually print them until the
-- process terminates.  Essentially, this is a thread-safe version of
-- 'runLogStdout'.
runLogStdoutC :: (MonadIO m) => LogIO l m a -> l -> m a
runLogStdoutC (LogIO m) l = do
  queue <- liftIO newTQueueIO
  let
    action s = atomically $ writeTQueue queue s
  liftIO . forkIO . forever $ do
    log <- atomically . readTQueue $ queue
    putStrLn log
  runReaderT m (l,action)

-- | Run a 'LogIO' that writes logs to a 'TQueue'.
runLogTQueue :: LogIO l m a -> l -> TQueue String -> m a
runLogTQueue (LogIO m) l queue =
  let action s = atomically $ writeTQueue queue s
  in runReaderT m (l,action)

-- | Transform a @'LogIO' l 'IO'@ action into an 'IO' action that has
-- the same behavior.  This is useful when you need to fork off a new
-- thread that should use the same logging system, for example.
passLogIO :: LogIO l IO a -> LogIO l IO (IO a)
passLogIO (LogIO m) = LogIO $ do
  (l,action) <- ask
  return (runLogIO (LogIO m) l action)

passLogIOF :: (a -> LogIO l IO b) -> LogIO l IO (a -> IO b)
passLogIOF f = LogIO $ do
  (l,action) <- ask
  return $ \a -> runLogIO (f a) l action
