/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.cache.lucene.internal.IndexRepositoryFactory.APACHE_GEODE_INDEX_COMPLETE;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import org.apache.geode.CancelException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.directory.DumpDirectoryFiles;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingFixedResolver;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingMap;
import org.apache.geode.cache.lucene.internal.partition.BucketTargetingResolver;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionRegionConfig;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;

public class LuceneIndexForPartitionedRegion extends LuceneIndexImpl {
  private Region fileAndChunkRegion;
  private final FileSystemStats fileSystemStats;

  public static final String FILES_REGION_SUFFIX = ".files";
  private static final String FILE_REGION_LOCK_FOR_BUCKET_ID = "FileRegionLockForBucketId:";

  public LuceneIndexForPartitionedRegion(String indexName, String regionPath, InternalCache cache) {
    super(indexName, regionPath, cache);

    String statsName = indexName + "-" + regionPath;
    this.fileSystemStats = new FileSystemStats(cache.getDistributedSystem(), statsName);
  }

  protected RepositoryManager createRepositoryManager(LuceneSerializer luceneSerializer) {
    return new PartitionedRepositoryManager(this,
        luceneSerializer == null ? new HeterogeneousLuceneSerializer() : luceneSerializer);
  }

  protected void createLuceneListenersAndFileChunkRegions(
      PartitionedRepositoryManager partitionedRepositoryManager) {
    partitionedRepositoryManager.setUserRegionForRepositoryManager((PartitionedRegion) dataRegion);
    final boolean withPersistence = withPersistence();
    RegionAttributes regionAttributes = dataRegion.getAttributes();
    final boolean withStorage = regionAttributes.getPartitionAttributes().getLocalMaxMemory() > 0;

    // create PR fileAndChunkRegion, but not to create its buckets for now
    final String fileRegionName = createFileRegionName();
    PartitionAttributes partitionAttributes = dataRegion.getPartitionAttributes();
    DistributionManager dm = this.cache.getInternalDistributedSystem().getDistributionManager();
    LuceneBucketListener lucenePrimaryBucketListener =
        new LuceneBucketListener(partitionedRepositoryManager, dm);

    if (!fileRegionExists(fileRegionName)) {
      fileAndChunkRegion =
          createRegion(fileRegionName, getRegionShortcut(withPersistence, withStorage),
              this.regionPath, partitionAttributes, regionAttributes, lucenePrimaryBucketListener);
    }

    fileSystemStats
        .setBytesSupplier(() -> getFileAndChunkRegion().getPrStats().getDataStoreBytesInUse());

  }

  private RegionShortcut getRegionShortcut(boolean withPersistence, boolean withStorage) {
    // TODO: 1) dataRegion should be withStorage
    // 2) Persistence to Persistence
    // 3) Replicate to Replicate, Partition To Partition
    // 4) Offheap to Offheap

    if (!withStorage) {
      return RegionShortcut.PARTITION_PROXY;
    } else if (withPersistence) {
      // TODO: add PartitionedRegionAttributes instead
      return RegionShortcut.PARTITION_PERSISTENT;
    } else {
      return RegionShortcut.PARTITION;
    }
  }

  public PartitionedRegion getFileAndChunkRegion() {
    return (PartitionedRegion) fileAndChunkRegion;
  }

  public FileSystemStats getFileSystemStats() {
    return fileSystemStats;
  }

  boolean fileRegionExists(String fileRegionName) {
    return cache.getRegion(fileRegionName) != null;
  }

  public String createFileRegionName() {
    return LuceneServiceImpl.getUniqueIndexRegionName(indexName, regionPath, FILES_REGION_SUFFIX);
  }

  private PartitionAttributes createLuceneRegionAttributesFromExistingAttributes(
      PartitionAttributesFactory attributesFactory,
      PartitionAttributes<?, ?> dataRegionAttributes) {
    attributesFactory.setTotalNumBuckets(dataRegionAttributes.getTotalNumBuckets());
    attributesFactory.setRedundantCopies(dataRegionAttributes.getRedundantCopies());
    attributesFactory.setPartitionResolver(getPartitionResolver(dataRegionAttributes));
    attributesFactory.setRecoveryDelay(dataRegionAttributes.getRecoveryDelay());
    attributesFactory.setStartupRecoveryDelay(dataRegionAttributes.getStartupRecoveryDelay());
    return attributesFactory.create();
  }

  private PartitionResolver getPartitionResolver(PartitionAttributes dataRegionAttributes) {
    if (dataRegionAttributes.getPartitionResolver() instanceof FixedPartitionResolver) {
      return new BucketTargetingFixedResolver();
    } else {
      return new BucketTargetingResolver();
    }
  }

  protected <K, V> Region<K, V> createRegion(final String regionName,
      final RegionShortcut regionShortCut, final String colocatedWithRegionName,
      final PartitionAttributes partitionAttributes, final RegionAttributes regionAttributes,
      PartitionListener lucenePrimaryBucketListener) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    if (lucenePrimaryBucketListener != null) {
      partitionAttributesFactory.addPartitionListener(lucenePrimaryBucketListener);
    }
    partitionAttributesFactory.setColocatedWith(colocatedWithRegionName);
    PartitionAttributes newPartitionAttributes = createLuceneRegionAttributesFromExistingAttributes(
        partitionAttributesFactory, partitionAttributes);

    // Create AttributesFactory based on input RegionShortcut
    RegionAttributes baseAttributes = this.cache.getRegionAttributes(regionShortCut.toString());
    AttributesFactory factory = new AttributesFactory(baseAttributes);
    factory.setPartitionAttributes(newPartitionAttributes);
    if (regionAttributes.getDataPolicy().withPersistence()) {
      factory.setDiskStoreName(regionAttributes.getDiskStoreName());
    }

    return createRegion(regionName, factory.create());
  }

  public void close() {}

  @Override
  public void dumpFiles(final String directory) {
    ResultCollector results = FunctionService.onRegion(getDataRegion())
        .setArguments(new String[] {directory, indexName}).execute(DumpDirectoryFiles.ID);
    results.getResult();
  }

  @Override
  public void destroy(boolean initiator) {
    if (logger.isDebugEnabled()) {
      logger.debug("Destroying index regionPath=" + regionPath + "; indexName=" + indexName
          + "; initiator=" + initiator);
    }

    // Invoke super destroy to remove the extension and async event queue
    super.destroy(initiator);

    // Destroy index on remote members if necessary
    if (initiator) {
      destroyOnRemoteMembers();
    }

    // Destroy the file region (colocated with the application region) if necessary
    // localDestroyRegion can't be used because locally destroying regions is not supported on
    // colocated regions
    if (initiator) {
      fileAndChunkRegion.destroyRegion();
      if (logger.isDebugEnabled()) {
        logger.debug("Destroyed fileAndChunkRegion=" + fileAndChunkRegion.getName());
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Destroyed index regionPath=" + regionPath + "; indexName=" + indexName
          + "; initiator=" + initiator);
    }
  }

  @Override
  public IndexRepository computeIndex(Integer bucketId, LuceneSerializer serializer,
      Region<?, ?> userRegion, IndexRepository oldRepository) throws IOException {
    final PartitionedRegion fileRegion = getFileAndChunkRegion();

    // We need to ensure that all members have created the fileAndChunk region before continuing
    Region prRoot = PartitionedRegionHelper.getPRRoot(fileRegion.getCache());
    PartitionRegionConfig prConfig =
        (PartitionRegionConfig) prRoot.get(fileRegion.getRegionIdentifier());
    while (!prConfig.isColocationComplete()) {
      prConfig = (PartitionRegionConfig) prRoot.get(fileRegion.getRegionIdentifier());
    }

    BucketRegion fileAndChunkBucket = fileRegion.getOrCreateMatchingBucketForRegion(bucketId);
    BucketRegion dataBucket =
        ((PartitionedRegion) userRegion).getOrCreateMatchingBucketForRegion(bucketId);

    if (fileAndChunkBucket == null || !fileAndChunkBucket.getBucketAdvisor().isPrimary()) {
      if (oldRepository != null) {
        oldRepository.cleanup();
      }
      return null;
    }

    if (oldRepository != null && !oldRepository.isClosed()) {
      return oldRepository;
    }

    if (oldRepository != null) {
      oldRepository.cleanup();
    }
    DistributedLockService lockService = getLockService();
    String lockName = getLockName(fileAndChunkBucket);
    while (!lockService.lock(lockName, 100, -1)) {
      if (!fileAndChunkBucket.getBucketAdvisor().isPrimary()) {
        return null;
      }
    }

    final IndexRepository repo;
    boolean initialPdxReadSerializedFlag = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    boolean success = false;
    try {
      // bucketTargetingMap handles partition resolver (via bucketId as callbackArg)
      Map bucketTargetingMap = getBucketTargetingMap(fileAndChunkBucket, bucketId);
      RegionDirectory dir = new RegionDirectory(bucketTargetingMap, this.getFileSystemStats());
      IndexWriterConfig config = new IndexWriterConfig(this.getAnalyzer());
      IndexWriter writer = new IndexWriter(dir, config);
      repo = new IndexRepositoryImpl(fileAndChunkBucket, writer, serializer, this.getIndexStats(),
          dataBucket, lockService, lockName, this);

      // fileRegion ops (get/put) need bucketId as a callbackArg for PartitionResolver
      if (null != fileRegion.get(APACHE_GEODE_INDEX_COMPLETE, bucketId)) {
        success = true;
        return repo;
      } else {
        success = reindexUserDataRegion(bucketId, userRegion, fileRegion, dataBucket, repo);
      }
      return repo;
    } catch (IOException e) {
      logger.info("Exception thrown while constructing Lucene Index for bucket:" + bucketId
          + " for file region:" + fileAndChunkBucket.getFullPath());
      throw e;
    } catch (CacheClosedException e) {
      logger.info("CacheClosedException thrown while constructing Lucene Index for bucket:"
          + bucketId + " for file region:" + fileAndChunkBucket.getFullPath());
      throw e;
    } finally {
      if (!success) {
        lockService.unlock(lockName);
        cache.setPdxReadSerializedOverride(initialPdxReadSerializedFlag);
      }
    }
  }

  private boolean reindexUserDataRegion(Integer bucketId, Region userRegion, Region fileRegion,
      BucketRegion dataBucket, IndexRepository repo) throws IOException {
    Set<IndexRepository> affectedRepos = new HashSet<IndexRepository>();

    Iterator keysIterator = dataBucket.keySet().iterator();
    while (keysIterator.hasNext()) {
      Object key = keysIterator.next();
      Object value = getValue(userRegion.getEntry(key));
      if (value != null) {
        repo.update(key, value);
      } else {
        repo.delete(key);
      }
      affectedRepos.add(repo);
    }

    for (IndexRepository affectedRepo : affectedRepos) {
      affectedRepo.commit();
    }
    // fileRegion ops (get/put) need bucketId as a callbackArg for PartitionResolver
    fileRegion.put(APACHE_GEODE_INDEX_COMPLETE, APACHE_GEODE_INDEX_COMPLETE, bucketId);
    return true;
  }

  private void destroyOnRemoteMembers() {
    PartitionedRegion pr = (PartitionedRegion) getDataRegion();
    DistributionManager dm = pr.getDistributionManager();
    Set<InternalDistributedMember> recipients = pr.getRegionAdvisor().adviseAllPRNodes();
    if (!recipients.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("LuceneIndexForPartitionedRegion: About to send destroy message recipients="
            + recipients);
      }
      ReplyProcessor21 processor = new ReplyProcessor21(dm, recipients);
      DestroyLuceneIndexMessage message = new DestroyLuceneIndexMessage(recipients,
          processor.getProcessorId(), regionPath, indexName);
      dm.putOutgoing(message);
      if (logger.isDebugEnabled()) {
        logger.debug("LuceneIndexForPartitionedRegion: Sent message recipients=" + recipients);
      }
      try {
        processor.waitForReplies();
      } catch (ReplyException e) {
        if (!(e.getCause() instanceof CancelException)) {
          throw e;
        }
      } catch (InterruptedException e) {
        dm.getCancelCriterion().checkCancelInProgress(e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private Map getBucketTargetingMap(BucketRegion region, int bucketId) {
    return new BucketTargetingMap(region, bucketId);
  }

  private String getLockName(final BucketRegion fileAndChunkBucket) {
    return FILE_REGION_LOCK_FOR_BUCKET_ID + fileAndChunkBucket.getFullPath();
  }

  private DistributedLockService getLockService() {
    return DistributedLockService
        .getServiceNamed(PartitionedRegionHelper.PARTITION_LOCK_SERVICE_NAME);
  }

  private Object getValue(Region.Entry entry) {
    final EntrySnapshot es = (EntrySnapshot) entry;
    Object value;
    try {
      value = es == null ? null : es.getRawValue(true);
    } catch (EntryDestroyedException e) {
      value = null;
    }
    return value;
  }
}
