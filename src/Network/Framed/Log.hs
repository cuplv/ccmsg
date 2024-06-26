{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

module Network.Framed.Log
  ( MonadLog (..)
  , LogIO
  , runLogIO
  , runLogStdout
  , runLogTQueue
  ) where

import Control.Concurrent.STM
import Control.Monad.Except
import Control.Monad.Reader

class (Monad m) => MonadLog l m | m -> l where
  dlog :: l -> String -> m ()
  -- ^ Log a 'String' at the given level.

-- | A 'MonadLog' that prints logs to stdout.
newtype LogPrint l m a = LogPrint { runLogPrint' :: ReaderT (l,String) m a }

instance (Functor m) => Functor (LogPrint l m) where
  fmap f (LogPrint m) = LogPrint (fmap f m)

instance (Applicative m) => Applicative (LogPrint l m) where
  pure = LogPrint . pure
  LogPrint f <*> LogPrint v = LogPrint (f <*> v)

instance (Monad m) => Monad (LogPrint l m) where
  LogPrint m >>= f = LogPrint (m >>= (\a -> runLogPrint' $ f a))

instance MonadTrans (LogPrint l) where
  lift m = LogPrint (lift m)

instance (MonadIO m) => MonadIO (LogPrint l m) where
  liftIO m = LogPrint (liftIO m)

instance (MonadError e m) => MonadError e (LogPrint l m) where
  throwError = lift . throwError
  catchError (LogPrint m) f = LogPrint $
    catchError m (\e -> runLogPrint' $ f e)

instance (MonadIO m, Ord l) => MonadLog l (LogPrint l m) where
  dlog l s = LogPrint $ do
    (l',prefix) <- ask
    if l <= l'
      then liftIO . putStrLn $ "[" ++ prefix ++ "] " ++ s
      else return ()

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

-- | Run a 'LogIO' that writes logs to a 'TQueue'.
runLogTQueue :: LogIO l m a -> l -> TQueue String -> m a
runLogTQueue (LogIO m) l queue =
  let action s = atomically $ writeTQueue queue s
  in runReaderT m (l,action)
