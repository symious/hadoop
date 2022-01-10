package org.apache.hadoop.yarn.server.federation.utils;

import org.apache.commons.lang3.NotImplementedException;

import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.Map;

public class CacheUtil {

  /**
   * Internal class that implements the CacheLoader interface that can be
   * plugged into the CacheManager to load objects into the cache for specified
   * keys.
   */
  public static class CacheLoaderImpl<K, V> implements CacheLoader<K, V> {
    @SuppressWarnings("unchecked")
    @Override
    public V load(K key) throws CacheLoaderException {
      try {
        CacheRequest<K, V>
            query = (CacheRequest<K, V>) key;
        assert query != null;
        return query.getValue();
      } catch (Throwable ex) {
        throw new CacheLoaderException(ex);
      }
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys)
        throws CacheLoaderException {
      // The FACADE does not use the Cache's getAll API. Hence this is not
      // required to be implemented
      throw new NotImplementedException("Code is not implemented");
    }
  }

  /**
   * Internal class that encapsulates the cache key and a function that returns
   * the value for the specified key.
   */
  public static class CacheRequest<K, V> {
    private K key;
    private Func<K, V> func;

    public CacheRequest(K key, Func<K, V> func) {
      this.key = key;
      this.func = func;
    }

    public V getValue() throws Exception {
      return func.invoke(key);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((key == null) ? 0 : key.hashCode());
      return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      CacheRequest<K, V>
          other = (CacheRequest<K, V>) obj;
      if (key == null) {
        if (other.key != null) {
          return false;
        }
      } else if (!key.equals(other.key)) {
        return false;
      }

      return true;
    }
  }

  /**
   * Encapsulates a method that has one parameter and returns a value of the
   * type specified by the TResult parameter.
   */
  public interface Func<T, TResult> {
    TResult invoke(T input) throws Exception;
  }
}
