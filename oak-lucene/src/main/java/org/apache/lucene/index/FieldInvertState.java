/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;

import org.apache.lucene.analysis.TokenStream; // javadocs
import org.apache.lucene.util.AttributeSource;

/**
 * This class tracks the number and position / offset parameters of terms
 * being added to the index. The information collected in this class is
 * also used to calculate the normalization factor for a field.
 * 
 * @lucene.experimental
 */
public final class FieldInvertState {
  String name;
  int position;
  int length;
  int numOverlap;
  int offset;
  int maxTermFrequency;
  int uniqueTermCount;
  float boost;
  AttributeSource attributeSource;

  /** Creates {code FieldInvertState} for the specified
   *  field name. */
  public FieldInvertState(String name) {
    this.name = name;
  }
  
  /** Creates {code FieldInvertState} for the specified
   *  field name and values for all fields. */
  public FieldInvertState(String name, int position, int length, int numOverlap, int offset, float boost) {
    this.name = name;
    this.position = position;
    this.length = length;
    this.numOverlap = numOverlap;
    this.offset = offset;
    this.boost = boost;
  }

  /**
   * Re-initialize the state
   */
  void reset() {
    position = 0;
    length = 0;
    numOverlap = 0;
    offset = 0;
    maxTermFrequency = 0;
    uniqueTermCount = 0;
    boost = 1.0f;
    attributeSource = null;
  }

  /**
   * Get the last processed term position.
   * @return the position
   */
  public int getPosition() {
    return position;
  }

  /**
   * Get total number of terms in this field.
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /** Set length value. */
  public void setLength(int length) {
    this.length = length;
  }
  
  /**
   * Get the number of terms with <code>positionIncrement == 0</code>.
   * @return the numOverlap
   */
  public int getNumOverlap() {
    return numOverlap;
  }

  /** Set number of terms with {@code positionIncrement ==
   *  0}. */
  public void setNumOverlap(int numOverlap) {
    this.numOverlap = numOverlap;
  }
  
  /**
   * Get end offset of the last processed term.
   * @return the offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * Get boost value. This is the cumulative product of
   * document boost and field boost for all field instances
   * sharing the same field name.
   * @return the boost
   */
  public float getBoost() {
    return boost;
  }

  /** Set boost value. */
  public void setBoost(float boost) {
    this.boost = boost;
  }

  /**
   * Get the maximum term-frequency encountered for any term in the field.  A
   * field containing "the quick brown fox jumps over the lazy dog" would have
   * a value of 2, because "the" appears twice.
   */
  public int getMaxTermFrequency() {
    return maxTermFrequency;
  }
  
  /**
   * Return the number of unique terms encountered in this field.
   */
  public int getUniqueTermCount() {
    return uniqueTermCount;
  }

  /** Returns the {@link AttributeSource} from the {@link
   *  TokenStream} that provided the indexed tokens for this
   *  field. */
  public AttributeSource getAttributeSource() {
    return attributeSource;
  }
  
  /**
   * Return the field's name
   */
  public String getName() {
    return name;
  }
}
