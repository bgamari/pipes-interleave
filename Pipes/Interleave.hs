module Pipes.Interleave ( interleave
                        , combine
                        , merge
                        ) where
                        
import Control.Applicative
import Data.List (sortBy)
import Data.Function (on)
import Data.Either (rights)
import Pipes

-- $setup
-- >>> import Pipes.Prelude
-- >>> import Data.Function
-- >>> import Data.Functor.Identity

-- | Interleave elements from a set of 'Producers' such that the interleaved
-- stream is increasing with respect to the given ordering.
-- 
-- >>> toList $ interleave compare [each [1,3..10], each [1,5..20]] 
-- [1,1,3,5,5,7,9,9,13,17]
-- 
interleave :: (Monad m, Functor m)
           => (a -> a -> Ordering) -> [Producer a m ()] -> Producer a m ()
interleave compare producers = do
    xs <- lift $ rights <$> mapM Pipes.next producers
    go xs
  where --go :: (Monad m, Functor m) => [(a, Producer a m ())] -> Producer a m ()
        go [] = return ()
        go xs = do let (a,producer):xs' = sortBy (compare `on` fst) xs
                   yield a
                   x' <- lift $ next producer
                   go $ either (const xs') (:xs') x'

-- | Given a stream of increasing elements, combine those equal under the 
-- given equality relation
--
-- >>> let append (k,v) (_,v') = return (k, v+v')
-- >>> toList $ combine ((==) `on` fst) append (each [(1,1), (1,4), (2,3), (3,10)])
-- [(1,5),(2,3),(3,10)]
--
combine :: (Monad m)
        => (a -> a -> Bool)    -- ^ equality test
        -> (a -> a -> m a)     -- ^ combine operation
        -> Producer a m r -> Producer a m r
combine eq append producer = lift (next producer) >>= either return (uncurry go)
  where go a producer' = do
          n <- lift $ next producer'
          case n of
            Left r                 -> yield a >> return r
            Right (a', producer'')
              | a `eq` a'          -> do a'' <- lift $ append a a'
                                         go a'' producer''
              | otherwise          -> yield a >> go a' producer''
   
-- | Equivalent to 'combine' composed with 'interleave'
--
-- >>> let append (k,v) (_,v') = return (k, v+v')
-- >>> let producers = [ each [(i,2) | i <- [1,3..10]], each [(i,10) | i <- [1,5..20]] ] :: [Producer (Int,Int) Identity ()]
-- >>> toList $ merge (compare `on` fst) append producers
-- [(1,12),(3,2),(5,12),(7,2),(9,12),(13,10),(17,10)]
-- 
merge :: (Monad m, Functor m)
      => (a -> a -> Ordering) -> (a -> a -> m a)
      -> [Producer a m ()] -> Producer a m ()
merge compare append =
    combine (\a b->compare a b == EQ) append . interleave compare
