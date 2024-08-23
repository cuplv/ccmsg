{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module Network.Ccm.State
  ( CcmState
  , ccmStats
  , CcmST
  , newCcmState
  , recordSend
  , localClock
  , MsgId
  , AppMsg
  , SimpleAppMsg (..)
  , mkSimpleAppMsg
  , mkCausalAppMsg
  , msgClock
  , msgPayload
  , CausalError (..)
  , showCausalError'
  , Stats
  , totalOutOfOrder
  , totalInOrder
  , CacheMode (..)
  ) where

import Network.Ccm.Bsm
import Network.Ccm.Lens
import Network.Ccm.Types
import Network.Ccm.VClock

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable (foldlM)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromJust)
import Data.Sequence (Seq,(<|),(|>))
import qualified Data.Sequence as Seq
import Data.Store
import Data.Store.TH
import Data.Traversable (for)
import GHC.Exts (toList,fromList)

type MsgId = (NodeId, SeqNum)

data CacheMode
  = CacheNone -- ^ Do not cache messages, disabling retransmission
  | CacheTemp -- ^ Cache all messages until they are universally delivered
  | CacheForever -- ^ Cache all messages forever
  deriving (Show,Eq,Ord)

data AppMsg
  = AppMsg { _msgClock :: VClock
           , _msgPayload :: ByteString
           }
  deriving (Show,Eq,Ord)

makeStore ''AppMsg

makeLenses ''AppMsg

data SimpleAppMsg
  = SimpleAppMsg ByteString
  deriving (Show,Eq,Ord)

makeStore ''SimpleAppMsg

mkSimpleAppMsg :: ByteString -> SimpleAppMsg
mkSimpleAppMsg = SimpleAppMsg

mkCausalAppMsg :: VClock -> ByteString -> AppMsg
mkCausalAppMsg = AppMsg

data CausalError
  = CausalError
    { errorMsgSender :: NodeId
    , errorLocalClock :: VClock
    , errorMsgClock :: VClock
    }
  deriving (Eq,Ord)

instance Show CausalError where
  show e =
    "[!]Causal: "
    ++ show (errorMsgSender e)
    ++ " "
    ++ show (errorMsgClock e)
    ++ " => "
    ++ show (errorLocalClock e)

{-| Show causal error, including receiver's ID -}
showCausalError' :: NodeId -> CausalError -> String
showCausalError' n e =
  "[!]Causal: "
  ++ show (errorMsgSender e)
  ++ " "
  ++ show (errorMsgClock e)
  ++ " => "
  ++ show n
  ++ " "
  ++ show (errorLocalClock e)

{-| Sequence number of a message, in terms of its sender and its clock. -}
msgSeqNum :: NodeId -> AppMsg -> SeqNum
msgSeqNum sender msg = nextNum sender (msg ^. msgClock)

msgId :: NodeId -> AppMsg -> MsgId
msgId sender msg = (sender, nextNum sender (msg^.msgClock))

data MsgCache
  = MsgCache
    { _mcDeleted :: MsgCount
    , _mcDelivered :: Seq AppMsg
    , _mcWaiting :: Seq AppMsg
    }
    deriving (Show, Eq, Ord)

makeLenses ''MsgCache

newMsgCache :: MsgCache
newMsgCache = MsgCache 0 Seq.Empty Seq.Empty

data Stats
  = Stats
    { _totalOutOfOrder :: Int
    , _totalInOrder :: Int
    }

makeLenses ''Stats

data CcmState
  = CcmState
    { _ccmCache :: Map NodeId MsgCache
    , _ccmBlocks :: Map NodeId (SeqNum, Seq NodeId)
    , _ccmReady :: Seq NodeId
    , _ccmPeerClocks :: Map NodeId VClock
    , _localClock :: VClock
    , _ccmStats :: Stats
    , _ccmCacheMode :: CacheMode
    }

type CcmST m = StateT CcmState m

makeLenses ''CcmState

blocks :: NodeId -> Lens' CcmState (Maybe (SeqNum, Seq NodeId))
blocks i = ccmBlocks . at i

cache :: NodeId -> Lens' CcmState MsgCache
cache i = ccmCache . at i . non newMsgCache

newCcmState :: CacheMode -> CcmState
newCcmState cacheMode = CcmState
  { _ccmCache = Map.empty
  , _ccmBlocks = Map.empty
  , _ccmReady = Seq.Empty
  , _ccmPeerClocks = Map.empty
  , _localClock = zeroClock
  , _ccmStats = Stats
    { _totalOutOfOrder = 0
    , _totalInOrder = 0
    }
  , _ccmCacheMode = cacheMode
  }

{- | Process a newly-received message.  If possible, the message is
   delivered immediately, and the payload is returned as a 'Right'
   value.  Otherwise, it is deferred and a 'CausalError' is returned
   to explain why. -}
intakeMessage
  :: (Monad m)
  => (NodeId, AppMsg)
  -> CcmST m (Either CausalError ByteString)
intakeMessage (sender,msg) = do
  r <- punchClock sender (msg^.msgClock)
  case r of
    Right () -> do
      ccmStats . totalInOrder += 1
      -- Put message in retransmission cache, if configured.
      cacheDelivered (sender,msg)
      return $ Right (msg^.msgPayload)
    Left m2 -> do
      ccmStats . totalOutOfOrder += 1
      cache sender . mcWaiting %= (|> msg)
      deferMsg sender m2
      local <- use localClock
      return . Left $ CausalError
        { errorMsgSender = sender
        , errorMsgClock = msg^.msgClock
        , errorLocalClock = local
        }

-- tryDeliverNext :: (Monad m) => CcmST m (Bool, Maybe ByteString)
-- tryDeliverNext = do
--   rdy <- use ccmReady
--   case rdy of
--     sender <|: rdy' -> do
--       ccmReady .= rdy'
--       w <- use $ cache sender . mcWaiting
--       case w of
--         msg <|: w' -> do
--           r <- punchClock sender (msg^.msgClock)
--           case r of
--             Right () -> do
--               cacheDelivered (sender,msg)
--               return (Right (msg^.msgPayload)
--     Seq.Empty -> return (False, Nothing)

{- | Cache a delivered message for later retransmission requests, if
   configured to do so. -}
cacheDelivered
  :: (Monad m)
  => (NodeId, AppMsg)
  -> CcmST m ()
cacheDelivered (sender, msg) = do
  m <- use ccmCacheMode
  case m of
    CacheNone -> return ()
    _ -> cache sender . mcDelivered %= (|> msg)

-- {-| Try to deliver a message. If this fails, delivery will be retried
--   later automatically. If it succeeds, the delivered message, and any
--   deferred messages that were delivered as a result, are returned in
--   causal order. -}
-- tryDeliver
--   :: (Monad m)
--   => (NodeId, AppMsg)
--   -> CcmST m (Either CausalError (VClock, Seq ByteString))
-- tryDeliver (sender,msg) = do
--   let m = msgId sender msg
--   r <- punchClock sender (msg^.msgClock)
--   case r of
--     Right () -> do
--       -- Retry delivery of deferred msgs, collecting ids of successful
--       -- retries.
--       retries <- retryLoop m

--       undefined

--       -- -- Get payloads of successfully retried msgs.
--       -- retryPayloads <- for retries $ \(rn,rs) -> do
--       --   undefined

--         -- -- Set m1's content to Nothing, returning the old value.
--         -- c <- ccmMsgStore . at m1 <<.= Nothing
--         -- case c of
--         --   Just c -> return (c^.msgPayload)
--         --   Nothing -> error $ "Deferred msg had no stored content " ++ show m
        

--       -- local <- use localClock
--       -- return $ Right (local, (msg^.msgPayload) <| retryPayloads)

--     Left m2 -> do
--       undefined

--       -- -- Store msg's content for retry (since it is being deferred for
--       -- -- the first time).
--       -- ccmMsgStore . at m .= Just msg
--       -- -- Register msg's id to be retried later.
--       -- deferMsgId m m2

--       -- local <- use localClock
--       -- return . Left $ CausalError
--       --   { errorMsgSender = sender
--       --   , errorMsgClock = msg^.msgClock
--       --   , errorLocalClock = local
--       --   }

{-| Run @deferMsgId i m@ when you receive a message from @n@ that
  cannot be delivered until after @m@ has been delivered. -}
deferMsg :: (Monad m) => NodeId -> MsgId -> CcmST m ()
deferMsg i1 (i2,sn2) =
  blocks i2 %= \v -> case v of
    Just (sn,d) -> Just (min sn sn2, d |> i1)
    Nothing -> Just (sn2, Seq.singleton i1)

  -- blocks i2 %= (\(n',d) -> (minMaybe (Just n2) n', d |> i1))
  -- let
  --   f v = case v of
  --     Just (n',d) ->
  --       Just (min n2 n', d |> m1)
  --     Nothing ->
  --       Just (n2, fromList [m1])
  -- in
  --   ccmWaiting . at i2 %= f

{- | Try to deliver a msg that has preveously been deferred.  To call
   this, provide the 'MsgId'.  We assume that the corresponding
   content is in 'ccmMsgStore'.  If delivery is successful this time,
   return 'True'.

   If deliver is unsuccessful, the 'MsgId' is re-deferred and then
   'False' is returned. -}
retryDeliver :: (Monad m) => NodeId -> CcmST m (Maybe (SeqNum, ByteString))
retryDeliver i = do
  -- This should consider the head of @cache i . mcWaiting@
  undefined
  -- mdep <- use $ ccmMsgStore . at m1
  -- dep <- case mdep of
  --   Just msg -> return $ msg^.msgClock
  --   Nothing -> do
  --     msgStore <- use ccmMsgStore
  --     error $
  --       "No stored clock for MsgId "
  --       ++ show m1
  --       ++ ", "
  --       ++ show msgStore
  -- result <- punchClock (fst m1) dep
  -- case result of
  --   Right () -> do
  --     return True
  --   Left m2 -> do
  --     deferMsgId m1 m2
  --     return False

{- | @punchClock i v@ attempts to update the local clock to include a
   new message from process @i@, which has dependencies @v@.

   If this fails, the local clock is unchanged and a message-id that
   has been witnessed by @v@ but not witnessed by the local clock is
   returned. -}
punchClock :: (Monad m) => NodeId -> VClock -> CcmST m (Either MsgId ())
punchClock msgSender msgClock = do
  local <- use localClock
  case leVC' msgClock local of
    Right () -> do
      localClock %= tick msgSender
      return $ Right ()
    Left m ->
      return $ Left m

{-| Record the sending of a new message, trusting that the new message's
  clock is satisfied by the local clock.

  TODO: The message should be cached for later re-send requests.  For
  now, only the local clock is modified. -}
recordSend :: (Monad m) => (NodeId, AppMsg) -> CcmST m ()
recordSend (sender,msg) = do
  localClock %= tick sender
  cacheDelivered (sender,msg)

-- {-| Cache a delivered message, so that it can be sent to other nodes
--   that ask for it later. -}
-- cacheMsg :: (Monad m) => (NodeId, AppMsg) -> CcmST m ()
-- cacheMsg (sender,msg) = do
--   cacheMode <- use ccmCacheMode

--   case cacheMode of
--     CacheNone -> return ()
--     _ -> do
--       ccmCache . at sender %=
--         (\v -> case v of
--             Nothing ->
--               -- If no cache yet exists for the sender, this must be
--               -- its first message.  So we place a 0 to indicate that
--               -- 0 messages have been garbage-collected so far.
--               Just (0, fromList [msg])
--             Just (seqNum,d) ->
--               -- Else, we preserve the existing number of
--               -- garbage-collected messages.
--               Just (seqNum,d |> msg))

-- {-| Return any 'MsgId's that were waiting on the given 'MsgId', removing
--   them from the internal waiting map. -}
-- revive :: (Monad m) => MsgId -> CcmST m (Seq NodeId)
-- revive (sender,sn) = do
--   bs <- use (blocks sender)
--   case bs of
--     Just (sn',nids) | sn' <= sn -> do
--       blocks sender .= Nothing
--       return nids
--     _ -> return Seq.Empty

-- {-| Retry delivery of any messages that depended on the given 'MsgId',
--   and any messages that depended on those messages, and so on.  Return
--   the newly-delivered 'MsgId's. -}
-- retryLoop :: (Monad m) => MsgId -> CcmST m (Seq MsgId, Seq ByteString)
-- retryLoop m = do
--   let f (is,bss) i1 = do
--         -- @retryDeliver m1@ automatically re-defers m1 when it fails.
--         result <- retryDeliver i1
--         case result of
--           Just (sn,bs) -> return (is |> (i1,sn), bss |> bs)
--           Nothing -> return (is, bss)
--         -- if result
--         --   then return $ is |> i1
--         --   else return $ is
--   -- Collect all 'MsgId's from any queue that was waiting on m.
--   is <- revive m
--   -- Retry delivery of revived 'MsgId's, re-deferring failures and
--   -- returning successes.
--   (msDlv, bsDlv) <- foldlM f Seq.empty is
--   -- Loop on successfully retried 'MsgId's.
--   loopMs <- traverse retryLoop msDlv
--   return $ foldMap id (msDlv <| loopMs)
