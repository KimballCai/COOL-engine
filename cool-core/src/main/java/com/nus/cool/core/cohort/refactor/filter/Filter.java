package com.nus.cool.core.cohort.refactor.filter;

import com.nus.cool.core.cohort.refactor.storage.Scope;
import com.nus.cool.core.io.readstore.MetaChunkRS;

import java.util.BitSet;
import java.util.List;

/** All filters should implement filter's interface */
public interface Filter {

  /**
   * Range Filter implements this function
   *
   * @param value the Input Value to check
   * @return true if the condition is met, otherwise false
   */
  public Boolean accept(Integer value) throws RuntimeException;

  /**
   * Range Filter implements this function
   *
   * @param values the batch of input value to check
   * @return List of bool stored in BitSet
   */
  public BitSet accept(List<Integer> values) throws RuntimeException;

  /**
   * Range Filter implements this function
   *
   * @param values Scope of time_min-time_max
   * @return whether all value in this scope can be accepted
   */
  public boolean accept(Scope values) throws RuntimeException;

  /**
   * @return the type of Filter
   */
  public FilterType getType();

  /**
   * @return the related Schema
   */
  public String getFilterSchema();

  /** Load meta information when get New Cublet Since the Gid is unique in every Cublet */
  public void loadMetaInfo(MetaChunkRS metaChunkRS);

  // ---------------- For compatiable ---------------
  public Boolean accept(String value) throws RuntimeException;

  public BitSet accept(String[] values) throws RuntimeException;
}
