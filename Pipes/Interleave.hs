{-# LANGUAGE ScopedTypeVariables #-}

module Pipes.Interleave ( interleave
                        , combine
                        , combineM
                        , merge
                        , mergeM
                        , groupBy
                        ) where

import Control.Monad (liftM)
import Data.Either (rights)
import qualified Data.Heap as H
import qualified Data.Sequence as Seq
import Data.Foldable (toList)
import Pipes

-- $setup
-- >>> import Pipes.Prelude
-- >>> import Data.Function
-- >>> import Data.Functor.Identity

-- | Interleave elements from a set of 'Producers' such that the interleaved
-- stream is increasing with respect to the given ordering.
--
-- >>> toList $ interleave [each [1,3..10], each [1,5..20]]
-- [1,1,3,5,5,7,9,9,13,17]
--
interleave :: forall a m. (Monad m, Ord a)
           => [Producer a m ()]      -- ^ element producers
           -> Producer a m ()
interleave producers = do
    xs <- lift $ rights `liftM` mapM Pipes.next producers
    go (H.fromList $ map (uncurry H.Entry) xs)
  where go :: (Monad m, Functor m) => H.Heap (H.Entry a (Producer a m ())) -> Producer a m ()
        go xs
          | Just (H.Entry a producer, xs') <- H.viewMin xs = do
              yield a
              x' <- lift $ next producer
              go $ either (const xs') (\(x,prod) -> H.insert (H.Entry x prod) xs') x'
          | otherwise = return ()
{-# INLINABLE interleave #-}

-- | Given a stream of increasing elements, combine those that are equal.
--
-- >>> let append (Entry k v) (Entry _ v') = Entry k (v+v')
-- >>> toList $ combine append (each $ map (uncurry Entry) [(1,1), (1,4), (2,3), (3,10)])
-- [Entry {priority = 1, payload = 5},Entry {priority = 2, payload = 3},Entry {priority = 3, payload = 10}]
--
combine :: (Monad m, Eq a)
        => (a -> a -> a)       -- ^ combine operation
        -> Producer a m r -> Producer a m r
combine append = combineM (\a b->return $ append a b)
{-# INLINEABLE combine #-}

-- | 'combine' with monadic side-effects in the combine operation.
combineM :: (Monad m, Eq a)
         => (a -> a -> m a)     -- ^ combine operation
         -> Producer a m r -> Producer a m r
combineM append producer = lift (next producer) >>= either return (uncurry go)
  where go a producer' = do
          n <- lift $ next producer'
          case n of
            Left r                 -> yield a >> return r
            Right (a', producer'')
              | a == a'            -> do a'' <- lift $ append a a'
                                         go a'' producer''
              | otherwise          -> yield a >> go a' producer''
{-# INLINABLE combineM #-}

-- | Equivalent to 'combine' composed with 'interleave'
--
-- >>> let append (Entry k v) (Entry _ v') = Entry k (v+v')
-- >>> let producers = [ each [Entry i 2 | i <- [1,3..10]], each [Entry i 10 | i <- [1,5..20]] ] :: [Producer (Entry Int Int) Identity ()]
-- >>> toList $ merge append producers
-- [(1,12),(3,2),(5,12),(7,2),(9,12),(13,10),(17,10)]
--
merge :: (Monad m, Ord a)
      => (a -> a -> a)           -- ^ combine operation
      -> [Producer a m ()]       -- ^ producers of elements
      -> Producer a m ()
merge append = mergeM (\a b->return $ append a b)
{-# INLINABLE merge #-}

-- | Merge with monadic side-effects in the combine operation.
mergeM :: (Monad m, Ord a)
       => (a -> a -> m a)         -- ^ combine operation
       -> [Producer a m ()]       -- ^ producers of elements
       -> Producer a m ()
mergeM append =
    combineM append . interleave
{-# INLINABLE mergeM #-}

-- | Split stream into groups of equal elements.
-- Note that this is a non-local operation: if the 'Producer' generates
-- a large run of equal elements, all of them will remain in memory until the
-- run ends.
--
-- >>> toList $ groupBy (each [Entry 1 1, Entry 1 4, Entry 2 3, Entry 3 10])
-- [[Entry {priority = 1, payload = 1},Entry {priority = 1, payload = 4}],[Entry {priority = 2, payload = 3}],[Entry {priority = 3, payload = 10}]]
--
groupBy :: forall a r m. (Monad m, Ord a)
        => Producer a m r -> Producer [a] m r
groupBy producer =
    lift (next producer) >>= either return (\(x,producer)->go (Seq.singleton x) producer)
  where
    go :: Seq.Seq a -> Producer a m r -> Producer [a] m r
    go xs producer' = do
      n <- lift $ next producer'
      case n of
        Left r                 -> yield (toList xs) >> return r
        Right (x, producer'')
          | x == x0            -> go (xs Seq.|> x) producer''
          | otherwise          -> yield (toList xs) >> go (Seq.singleton x) producer''
          where x0 Seq.:< _ = Seq.viewl xs
{-# INLINABLE groupBy #-}
