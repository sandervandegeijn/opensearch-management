import time
from typing import List, Dict, Any, Optional, Union
from settings import Settings
from loguru import logger

class Ilm:
    """Manages OpenSearch index lifecycle with size-based rollover and searchable snapshots"""

    def __init__(self, settings: Settings) -> None:
        self.hot_storage_days: int = settings.number_of_days_on_hot_storage
        self.total_retention_days: int = settings.number_of_days_total_retention
        self.rollover_size_gb: int = settings.rollover_size_gb
        self.rollover_age_days: int = settings.rollover_age_days
        self.managed_index_patterns: tuple = settings.managed_index_patterns
        
        self.requests = settings.get_requests_object()
        self.base_url: str = settings.url
        
        # Validate configuration
        self._validate_configuration()

    # === PRIMARY ILM OPERATIONS (executed in order) ===

    def transition_old_indices_to_snapshots(self) -> None:
        """Move old indices to searchable snapshots"""
        # Skip snapshot phase if hot storage == total retention
        if self.hot_storage_days >= self.total_retention_days:
            logger.info("Hot storage period equals total retention - skipping snapshot phase")
            return
            
        logger.info(f"Moving indices older than {self.hot_storage_days} days to snapshots")
        
        for index_info in self.get_indices():
            index_name = index_info['index']
            
            if not self._should_manage_index(index_name):
                continue
                
            if self._is_ready_for_snapshot(index_name):
                logger.info(f"Processing {index_name} for snapshot")
                self._snapshot_and_replace_index(index_name)

    def cleanup_old_data(self) -> None:
        """Delete indices and snapshots past retention period using three-phase approach"""
        logger.info(f"Cleaning up data older than {self.total_retention_days} days")
        
        # Phase 1: Delete searchable snapshot indices older than retention
        logger.info("Phase 1: Checking searchable snapshot indices...")
        indices = self.get_indices()
        for index_info in indices:
            index_name = index_info['index']
            if index_name.endswith('-snapshot'):
                # Extract base index name to check if it should be managed
                base_index_name = index_name.replace('-snapshot', '')
                if self._should_manage_index(base_index_name):
                    age_days = self._get_searchable_snapshot_age_days(index_name)
                    if age_days >= self.total_retention_days:
                        logger.info(f"Deleting old searchable snapshot index {index_name} ({age_days:.1f} days old)")
                        self._delete_index(index_name)

        # Phase 2: Delete regular managed indices older than retention, then their corresponding snapshots
        logger.info("Phase 2: Checking regular indices...")
        for index_info in indices:
            index_name = index_info['index']
            if not self._should_manage_index(index_name):
                continue
            if index_name.endswith('-snapshot'):  # Skip searchable snapshots, handled in phase 1
                continue
                
            age_days = self._get_index_age_days(index_name)
            if age_days >= self.total_retention_days:
                logger.info(f"Deleting old index {index_name} ({age_days:.1f} days old)")
                self._delete_index(index_name)
                
                # Also try to delete corresponding snapshot
                corresponding_snapshot = self._get_corresponding_snapshot_name(index_name)
                if corresponding_snapshot:
                    logger.info(f"Deleting corresponding snapshot {corresponding_snapshot}")
                    self._delete_snapshot(corresponding_snapshot)

        # Phase 3: For each old snapshot, unmount any searchable snapshot indices referencing it, then delete snapshot
        logger.info("Phase 3: Checking snapshots...")
        snapshots = self.get_snapshots()
        for snapshot in snapshots:
            snapshot_age = self._snapshot_age_days(snapshot)
            
            # Skip snapshots with unknown ages
            if snapshot_age < 0:
                logger.debug(f"Snapshot {snapshot['id']} has unknown age; skipping age-based deletion")
                continue
            
            # Sanity check for unreasonable ages
            if snapshot_age >= 20000:  # ~55 years
                logger.warning(
                    f"Snapshot {snapshot['id']} computed age={snapshot_age:.1f}d — likely missing/zero end_epoch; "
                    f"verify snapshot state and fields (endEpoch/startEpoch)"
                )
                continue
                
            if snapshot_age >= self.total_retention_days:
                logger.info(f"Deleting old snapshot {snapshot['id']} ({snapshot_age:.1f} days old)")
                self._delete_snapshot_with_cleanup(snapshot['id'])

    def restore_missing_searchable_snapshots(self) -> None:
        """Restore searchable snapshots for all regular snapshots that don't have corresponding indices"""
        logger.info("Checking for missing searchable snapshots and restoring them")
        
        existing_indices = {idx['index'] for idx in self.get_indices()}
        restored_count = 0
        
        for snapshot in self.get_snapshots():
            if snapshot["status"] != "SUCCESS":
                continue
            
            # Check snapshot age - skip if too old or too young
            snapshot_age = self._snapshot_age_days(snapshot)
            if snapshot_age < 0:
                logger.info(f"Skipping snapshot {snapshot['id']} - could not determine age")
                continue
                
            # Skip snapshots that are too young (still in hot storage period)
            if snapshot_age < self.hot_storage_days:
                logger.info(f"Skipping snapshot {snapshot['id']} - too young ({snapshot_age:.1f} < {self.hot_storage_days} days)")
                continue
                
            snapshot_name = snapshot['id']
            
            # Get snapshot details to see what indices it contains
            try:
                details = self.requests.get(f"{self.base_url}/_snapshot/data/{snapshot_name}").json()
                if not details.get("snapshots"):
                    continue
                    
                snapshot_indices = details["snapshots"][0]["indices"]
                should_restore = False
                logger.debug(f"Checking snapshot {snapshot_name} with {len(snapshot_indices)} indices: {snapshot_indices}")
                
                for index_name in snapshot_indices:
                    if index_name.startswith(".ds"):  # Skip data stream backing indices
                        continue
                        
                    # Check if we should manage this index
                    if not self._should_manage_index(index_name):
                        continue
                    
                    searchable_name = f"{index_name}-snapshot"
                    base_exists = index_name in existing_indices
                    searchable_exists = searchable_name in existing_indices
                    
                    logger.debug(f"  Index '{index_name}': base_exists={base_exists}, searchable_exists={searchable_exists}")
                    logger.debug(f"  Searchable name: '{searchable_name}'")
                    
                    # Restore ONLY if neither base index nor searchable snapshot mount exists
                    if not base_exists and not searchable_exists:
                        logger.debug(f"  -> WILL RESTORE: Neither base nor searchable exists")
                        should_restore = True
                        break
                    else:
                        logger.debug(f"  -> SKIP: Base or searchable already exists")
                
                if should_restore:
                    logger.info(f"Restoring missing searchable snapshot from snapshot {snapshot_name}")
                    self._restore_as_searchable(snapshot_name, existing_indices)
                    restored_count += 1
                        
            except Exception as e:
                logger.error(f"Error checking snapshot {snapshot_name}: {e}")
        
        logger.info(f"Restored {restored_count} missing searchable snapshots")

    # === SIZE-BASED ROLLOVER OPERATIONS (independent job) ===

    def check_and_rollover_by_size(self) -> None:
        """Check write indices and rollover based on OpenSearch conditions"""
        logger.info(f"Checking for rollover (threshold: {self.rollover_size_gb}GB size or {self.rollover_age_days} days age)")
        
        for alias_name in self._get_write_aliases():
            write_index = self._get_write_index(alias_name)
            if not write_index:
                continue
                
            logger.info(f"Checking rollover for {alias_name} -> {write_index}")
            
            # Let OpenSearch decide rollover based on max_size condition
            result = self._rollover_alias(alias_name)
            if result:
                logger.info(f"Rollover completed for {alias_name}")
            else:
                logger.debug(f"No rollover needed for {alias_name} (conditions not met)")

    # === EMERGENCY/MAINTENANCE OPERATIONS ===

    def remove_searchable_snapshots(self) -> None:
        """Emergency function to remove all searchable snapshot indices"""
        logger.warning("Removing all searchable snapshot indices")
        
        for index_info in self.get_indices():
            index_name = index_info['index']
            if not self._should_manage_index(index_name):
                continue
                
            if self._is_searchable_snapshot(index_name):
                logger.warning(f"Removing searchable snapshot: {index_name}")
                self._delete_index(index_name)

    # === PUBLIC UTILITY METHODS ===

    def get_indices(self) -> List[Dict[str, Any]]:
        """Get all indices"""
        logger.debug("Fetching all indices via API")
        try:
            response = self.requests.get(f"{self.base_url}/_cat/indices?format=json")
            response.raise_for_status()
            indices = response.json()
            logger.info(f"Retrieved {len(indices)} indices from OpenSearch")
            return indices
        except Exception as e:
            logger.error(f"Failed to fetch indices: {e}")
            return []
    
    def get_snapshots(self) -> List[Dict[str, Any]]:
        """Get all snapshots"""
        logger.debug("Fetching all snapshots via API")
        try:
            response = self.requests.get(f"{self.base_url}/_cat/snapshots/data?v&s=endEpoch&format=json", timeout=3600)
            response.raise_for_status()
            snapshots = response.json()
            logger.debug(f"Retrieved {len(snapshots)} snapshots from OpenSearch")
            return snapshots
        except Exception as e:
            logger.error(f"Failed to fetch snapshots: {e}")
            return []

    def get_managed_indices(self) -> List[Dict[str, Any]]:
        """Get indices matching managed patterns using pattern-based filtering"""
        all_managed_indices = []
        
        for pattern in self.managed_index_patterns:
            logger.debug(f"Fetching indices for pattern: {pattern}*")
            pattern_indices = self._get_indices_by_pattern(f"{pattern}*")
            all_managed_indices.extend(pattern_indices)
            logger.debug(f"Found {len(pattern_indices)} indices matching pattern {pattern}*")
        
        # Remove duplicates (in case patterns overlap)
        seen_indices = set()
        unique_indices = []
        for index_info in all_managed_indices:
            index_name = index_info['index']
            if index_name not in seen_indices:
                seen_indices.add(index_name)
                unique_indices.append(index_info)
        
        logger.info(f"Retrieved {len(unique_indices)} managed indices total from OpenSearch")
        return unique_indices

    # === PRIVATE SNAPSHOT OPERATIONS ===

    def _snapshot_and_replace_index(self, index_name: str, max_retries: int = 3) -> None:
        """Create snapshot and replace with searchable snapshot (with retry/cleanup)"""
        logger.info(f"Starting snapshot and replace process for {index_name}")
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{max_retries} for {index_name}")
                
                # Create snapshot with validation
                if self._create_snapshot_with_validation(index_name):
                    logger.info(f"Snapshot validation successful for {index_name}, proceeding to create searchable snapshot")
                    
                    # Create searchable snapshot
                    if self._create_searchable_snapshot(index_name):
                        # Only delete original index if BOTH operations succeeded
                        logger.info(f"Searchable snapshot created successfully for {index_name}, deleting original index")
                        self._delete_index(index_name)
                        logger.info(f"Successfully completed snapshot and replace for {index_name}")
                        return
                    else:
                        # Searchable snapshot failed - cleanup and retry
                        logger.warning(f"Searchable snapshot creation failed for {index_name}, attempt {attempt + 1}/{max_retries}")
                        self._cleanup_failed_snapshot(index_name)
                else:
                    # Regular snapshot failed - cleanup and retry  
                    logger.warning(f"Snapshot creation/validation failed for {index_name}, attempt {attempt + 1}/{max_retries}")
                    self._cleanup_failed_snapshot(index_name)
                    
                # Add delay before retry (reduced exponential backoff)
                if attempt < max_retries - 1:
                    delay_seconds = 30 * (2 ** attempt)  # 30s, 60s, 120s
                    delay_minutes = delay_seconds / 60
                    logger.info(f"Waiting {delay_minutes:.1f} minutes before retry {attempt + 2}/{max_retries} for {index_name}")
                    time.sleep(delay_seconds)
                    
            except Exception as e:
                logger.error(f"Unexpected error during snapshot process for {index_name}, attempt {attempt + 1}/{max_retries}: {e}")
                self._cleanup_failed_snapshot(index_name)
                
                # Add delay before retry on exception (reduced exponential backoff)
                if attempt < max_retries - 1:
                    delay_seconds = 30 * (2 ** attempt)  # 30s, 60s, 120s
                    delay_minutes = delay_seconds / 60
                    logger.info(f"Waiting {delay_minutes:.1f} minutes before retry {attempt + 2}/{max_retries} for {index_name}")
                    time.sleep(delay_seconds)
        
        # All retries exhausted
        logger.error(f"Failed to complete snapshot and replace for {index_name} after {max_retries} attempts - PRESERVING ORIGINAL INDEX")
        logger.error(f"Manual investigation required for {index_name} - check cluster health, storage capacity, and shard allocation")

    def _create_snapshot_with_validation(self, index_name: str) -> bool:
        """Create snapshot of index with validation"""
        if not self._create_snapshot(index_name):
            return False
        return self._validate_snapshot_health(index_name)

    def _create_snapshot(self, index_name: str) -> bool:
        """Create snapshot of index with polling"""
        body = {"indices": [index_name], "partial": False}
        
        # Create snapshot without waiting for completion
        response = self.requests.put(
            f"{self.base_url}/_snapshot/data/{index_name}", 
            json=body
        )
        
        if response.status_code == 400:
            # Snapshot already exists - check if it's complete
            logger.info(f"Snapshot {index_name} already exists, checking status")
            return self._wait_for_snapshot_completion(index_name)
        elif response.status_code != 200:
            logger.error(f"Snapshot creation failed: {index_name} (HTTP {response.status_code}) - Response: {response.text}")
            return False
        
        logger.info(f"Snapshot creation initiated for: {index_name}")
        return self._wait_for_snapshot_completion(index_name)

    def _wait_for_snapshot_completion(self, snapshot_name: str, max_wait_minutes: int = 60) -> bool:
        """Poll snapshot status until completion"""
        poll_interval_seconds = 30
        max_polls = (max_wait_minutes * 60) // poll_interval_seconds
        
        logger.info(f"Polling snapshot status for {snapshot_name} (max wait: {max_wait_minutes} minutes)")
        
        for poll_count in range(max_polls):
            try:
                response = self.requests.get(f"{self.base_url}/_snapshot/data/{snapshot_name}/_status")
                if response.status_code != 200:
                    logger.error(f"Failed to get snapshot status for {snapshot_name} (HTTP {response.status_code}) - Response: {response.text}")
                    return False
                
                status_data = response.json()
                if not status_data.get("snapshots"):
                    logger.error(f"No snapshot data found in status response for {snapshot_name}")
                    return False
                
                snapshot_status = status_data["snapshots"][0]
                state = snapshot_status.get("state", "UNKNOWN")
                
                logger.debug(f"Snapshot {snapshot_name} state: {state} (poll {poll_count + 1}/{max_polls})")
                
                if state == "SUCCESS":
                    logger.info(f"Snapshot {snapshot_name} completed successfully")
                    return True
                elif state == "PARTIAL":
                    logger.warning(f"Snapshot {snapshot_name} completed with state: PARTIAL")
                    return True
                elif state == "FAILED":
                    logger.error(f"Snapshot {snapshot_name} failed")
                    return False
                elif state in ["IN_PROGRESS", "STARTED", "INIT"]:
                    # Continue polling for all in-progress states
                    logger.debug(f"Snapshot {snapshot_name} in progress (state: {state})...")
                    time.sleep(poll_interval_seconds)
                    continue
                else:
                    logger.warning(f"Snapshot {snapshot_name} has unexpected state: {state}")
                    time.sleep(poll_interval_seconds)
                    continue
                    
            except Exception as e:
                logger.error(f"Error polling snapshot status for {snapshot_name}: {e}")
                return False
        
        # Timeout reached
        logger.error(f"Timeout waiting for snapshot {snapshot_name} to complete after {max_wait_minutes} minutes")
        return False

    def _validate_snapshot_health(self, snapshot_name: str) -> bool:
        """Validate that snapshot is complete and healthy"""
        try:
            response = self.requests.get(f"{self.base_url}/_snapshot/data/{snapshot_name}")
            if response.status_code != 200:
                logger.error(f"Cannot retrieve snapshot info for {snapshot_name} (HTTP {response.status_code}) - Response: {response.text}")
                return False
                
            snapshot_data = response.json()
            snapshot_info = snapshot_data["snapshots"][0]
            
            state = snapshot_info["state"]
            
            # Accept SUCCESS and PARTIAL for restore, only SUCCESS for creation
            if state == "SUCCESS":
                # For SUCCESS state, failures array should be empty, but log if not
                if snapshot_info.get("failures", []):
                    logger.warning(f"Snapshot {snapshot_name} has SUCCESS state but contains failures: {snapshot_info['failures']}")
                return True
            elif state == "PARTIAL":
                # PARTIAL snapshots have some failed shards but are still restorable
                logger.warning(f"Snapshot {snapshot_name} state: PARTIAL - some shards failed but snapshot is usable for restore")
                
                # Log detailed failure information as warnings
                if "failures" in snapshot_info and snapshot_info["failures"]:
                    logger.warning(f"Snapshot {snapshot_name} failures: {snapshot_info['failures']}")
                    
                # Log shard failure details if available
                if "shards" in snapshot_info:
                    shards = snapshot_info["shards"]
                    failed_shards = shards.get("failed", 0)
                    total_shards = shards.get("total", 0)
                    successful_shards = shards.get("successful", 0)
                    logger.warning(f"Snapshot {snapshot_name} shard status: {successful_shards}/{total_shards} successful, {failed_shards} failed")
                
                return True
            else:
                # FAILED, IN_PROGRESS, etc. - not acceptable
                logger.error(f"Snapshot {snapshot_name} state: {state}")
                
                # Log detailed failure information
                if "failures" in snapshot_info and snapshot_info["failures"]:
                    logger.error(f"Snapshot {snapshot_name} failures: {snapshot_info['failures']}")
                    
                # Log shard failure details if available
                if "shards" in snapshot_info:
                    shards = snapshot_info["shards"]
                    failed_shards = shards.get("failed", 0)
                    total_shards = shards.get("total", 0)
                    successful_shards = shards.get("successful", 0)
                    logger.error(f"Snapshot {snapshot_name} shard status: {successful_shards}/{total_shards} successful, {failed_shards} failed")
                    
                return False
        except Exception as e:
            logger.error(f"Error validating snapshot {snapshot_name}: {e}")
            return False

    def _cleanup_failed_snapshot(self, index_name: str) -> None:
        """Clean up failed snapshot artifacts"""
        logger.info(f"Cleaning up failed snapshot artifacts for {index_name}")
        
        # Clean up searchable snapshot index FIRST (if it exists)
        # Must delete this before the snapshot, as snapshots backing indices cannot be deleted
        searchable_name = f"{index_name}-snapshot"
        if self._index_exists(searchable_name):
            logger.info(f"Removing existing searchable snapshot index: {searchable_name}")
            self._delete_index(searchable_name)
        else:
            logger.debug(f"Index {searchable_name} does not exist, skipping deletion")
            
        # THEN clean up snapshot if it exists (now safe since no indices are using it)
        if self._snapshot_exists(index_name):
            self._delete_snapshot(index_name)
        else:
            logger.debug(f"Snapshot {index_name} does not exist, skipping deletion")

    def _create_searchable_snapshot(self, index_name: str) -> bool:
        """Create searchable snapshot from regular snapshot"""
        searchable_name = f"{index_name}-snapshot"
        
        # Check if searchable snapshot already exists
        if self._index_exists(searchable_name):
            # Verify it's actually a searchable snapshot (not just a regular index with same name)
            if self._is_searchable_snapshot(searchable_name):
                logger.info(f"Searchable snapshot already exists: {searchable_name}")
                return True
            else:
                logger.warning(f"Index {searchable_name} exists but is not a searchable snapshot - removing it")
                self._delete_index(searchable_name)
        
        body = {
            "indices": index_name,
            "rename_pattern": f"^{index_name}$",
            "rename_replacement": searchable_name,
            "storage_type": "remote_snapshot",
            "index_settings": {"index.number_of_replicas": 0}
        }
        
        response = self.requests.post(f"{self.base_url}/_snapshot/data/{index_name}/_restore", json=body)
        if response.status_code == 200:
            logger.info(f"Mounted snapshot as searchable index: {searchable_name}")
            return True
        else:
            logger.error(f"Failed to mount snapshot as searchable index: {searchable_name} (HTTP {response.status_code}) - Response: {response.text}")
            return False

    def _restore_as_searchable(self, snapshot_name: str, existing_indices: set = None) -> None:
        """Restore snapshot as searchable snapshot"""
        try:
            details = self.requests.get(f"{self.base_url}/_snapshot/data/{snapshot_name}").json()
            if existing_indices is None:
                existing_indices = {idx['index'] for idx in self.get_indices()}
            
            for index_name in details["snapshots"][0]["indices"]:
                if index_name.startswith(".ds"):
                    continue
                    
                searchable_name = f"{index_name}-snapshot"
                if searchable_name in existing_indices:
                    continue
                    
                body = {
                    "indices": [index_name],
                    "storage_type": "remote_snapshot",
                    "rename_pattern": "^(.*)",
                    "rename_replacement": "$1-snapshot",
                    "index_settings": {"index.number_of_replicas": 0}
                }
                
                response = self.requests.post(f"{self.base_url}/_snapshot/data/{snapshot_name}/_restore", json=body)
                if response.status_code == 200:
                    logger.info(f"Mounted snapshot as searchable index: {searchable_name}")
                else:
                    logger.error(f"Failed to mount snapshot as searchable index {searchable_name} (HTTP {response.status_code}) - Response: {response.text}")
        except Exception as e:
            logger.error(f"Error restoring snapshot {snapshot_name}: {e}")

    # === PRIVATE ROLLOVER OPERATIONS ===

    def _rollover_alias(self, alias_name: str) -> bool:
        """Rollover write alias based on size and age conditions"""
        body = {
            "conditions": {
                "max_size": f"{self.rollover_size_gb}gb",
                "max_age": f"{self.rollover_age_days}d"
            }
        }
        response = self.requests.post(f"{self.base_url}/{alias_name}/_rollover", json=body)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('rolled_over'):
                old = result.get('old_index')
                new = result.get('new_index')
                logger.info(f"Rolled over: {old} -> {new}")
                return True
            else:
                # OpenSearch determined rollover wasn't needed
                logger.debug(f"OpenSearch determined rollover wasn't needed for conditions")
                return False
        return False

    def _get_write_aliases(self) -> List[str]:
        """Get all write aliases"""
        try:
            response = self.requests.get(f"{self.base_url}/_alias")
            if response.status_code != 200:
                logger.error(f"Error getting aliases: HTTP {response.status_code} - Response: {response.text}")
                return []
            
            write_aliases = set()
            for index_data in response.json().values():
                if 'aliases' not in index_data:
                    continue
                for alias_name, config in index_data['aliases'].items():
                    if alias_name.endswith('-write') and config.get('is_write_index', False):
                        write_aliases.add(alias_name)
            
            return list(write_aliases)
        except Exception as e:
            logger.error(f"Error getting write aliases: {e}")
            return []

    def _get_write_index(self, alias_name: str) -> Optional[str]:
        """Get write index for alias"""
        try:
            response = self.requests.get(f"{self.base_url}/_alias/{alias_name}")
            if response.status_code != 200:
                logger.error(f"Error getting write index for {alias_name}: HTTP {response.status_code} - Response: {response.text}")
                return None
            
            for index_name, index_info in response.json().items():
                if 'aliases' in index_info:
                    for config in index_info['aliases'].values():
                        if config.get('is_write_index', False):
                            return index_name
            return None
        except Exception as e:
            logger.error(f"Error getting write index for {alias_name}: {e}")
            return None

    def _add_write_alias(self, index_name: str, alias_name: str) -> bool:
        """Add write alias to index"""
        body = {
            "actions": [{
                "add": {
                    "index": index_name,
                    "alias": alias_name,
                    "is_write_index": True
                }
            }]
        }
        
        response = self.requests.post(f"{self.base_url}/_aliases", json=body)
        return response.status_code == 200

    # === PRIVATE INDEX OPERATIONS ===
    
    def _delete_index(self, index_name: str) -> None:
        """Delete index"""
        response = self.requests.delete(f"{self.base_url}/{index_name}")
        if response.status_code == 200:
            logger.info(f"Deleted index: {index_name}")
        elif response.status_code == 404:
            logger.debug(f"Index {index_name} already deleted or does not exist")
        else:
            logger.error(f"Failed to delete index {index_name} (HTTP {response.status_code}) - Response: {response.text}")
    
    def _delete_snapshot(self, snapshot_name: str) -> None:
        """Delete snapshot"""
        response = self.requests.delete(f"{self.base_url}/_snapshot/data/{snapshot_name}")
        if response.status_code == 200:
            logger.info(f"Deleted snapshot: {snapshot_name}")
        elif response.status_code == 404:
            logger.debug(f"Snapshot {snapshot_name} already deleted or does not exist")
        else:
            logger.error(f"Failed to delete snapshot {snapshot_name} (HTTP {response.status_code}) - Response: {response.text}")

    def _delete_snapshot_with_cleanup(self, snapshot_name: str) -> None:
        """Delete snapshot, handling searchable snapshots by removing backing indices first"""
        # Check if this is a searchable snapshot (ends with -snapshot)
        if snapshot_name.endswith("-snapshot"):
            # For searchable snapshots, delete the backing index first
            if self._index_exists(snapshot_name):
                logger.info(f"Removing searchable snapshot index before deleting snapshot: {snapshot_name}")
                self._delete_index(snapshot_name)
        
        # Now delete the snapshot
        self._delete_snapshot(snapshot_name)

    def _create_index_with_alias(self, index_name: str, alias_name: str) -> bool:
        """Create new index with write alias"""
        body = {
            "aliases": {
                alias_name: {"is_write_index": True}
            }
        }
        response = self.requests.put(f"{self.base_url}/{index_name}", json=body)
        return response.status_code in [200, 201]

    # === PRIVATE QUERY/VALIDATION METHODS ===

    def _should_manage_index(self, index_name: str) -> bool:
        """Check if we should manage this index"""
        if index_name.endswith("-write"):
            return False
        return index_name.startswith(self.managed_index_patterns)
    
    def _is_ready_for_snapshot(self, index_name: str) -> bool:
        """Check if index is ready to be moved to snapshot"""
        if self._is_write_index(index_name):
            return False
        if self._is_searchable_snapshot(index_name):
            return False
        return self._get_index_age_days(index_name) >= self.hot_storage_days

    def _is_write_index(self, index_name: str) -> bool:
        """Check if index is currently a write index"""
        try:
            response = self.requests.get(f"{self.base_url}/{index_name}/_alias")
            if response.status_code != 200:
                return False
            
            alias_data = response.json()
            if index_name in alias_data:
                aliases = alias_data[index_name].get('aliases', {})
                return any(config.get('is_write_index', False) for config in aliases.values())
            return False
        except Exception as e:
            logger.debug(f"Error checking if {index_name} is write index: {e}")
            return False

    def _is_searchable_snapshot(self, index_name: str) -> bool:
        """Check if index is a searchable snapshot"""
        try:
            response = self.requests.get(f"{self.base_url}/{index_name}/_settings")
            if response.status_code != 200:
                return False
            settings = response.json()
            store_type = settings[index_name]["settings"]["index"].get("store", {}).get("type")
            return store_type == "remote_snapshot"
        except Exception as e:
            logger.debug(f"Error checking if {index_name} is searchable snapshot: {e}")
            return False

    def _snapshot_age_days(self, snap: Dict[str, Any]) -> float:
        """
        Compute snapshot age in days; robust to missing/zero end time.
        Returns -1.0 for unknown/invalid ages.
        """
        # Step 1: Try endEpoch / end_epoch fields
        end_epoch = snap.get("end_epoch", snap.get("endEpoch"))
        try:
            end_epoch = int(end_epoch) if end_epoch is not None else None
        except (ValueError, TypeError):
            end_epoch = None
        
        # Step 2: If end_epoch is invalid or 0, try startEpoch/start_epoch
        if not end_epoch:  # Treats 0, None, or empty as invalid
            start_epoch = snap.get("start_epoch", snap.get("startEpoch"))
            try:
                start_epoch = int(start_epoch) if start_epoch is not None else None
            except (ValueError, TypeError):
                start_epoch = None
            
            if start_epoch:
                end_epoch = start_epoch  # Use start as fallback
        
        # Step 3: Final fallback - query detailed snapshot API
        if not end_epoch:
            try:
                details = self.requests.get(f"{self.base_url}/_snapshot/data/{snap['id']}").json()
                info = (details.get("snapshots") or [{}])[0]
                
                # Try end_time_in_millis first
                et_ms = info.get("end_time_in_millis")
                if et_ms and et_ms > 0:
                    end_epoch = int(et_ms) // 1000
                else:
                    # Try start_time_in_millis as final fallback
                    st_ms = info.get("start_time_in_millis")
                    if st_ms and st_ms > 0:
                        end_epoch = int(st_ms) // 1000
            except Exception:
                pass  # Ignore API errors
        
        # Step 4: Return result or signal unknown age
        if not end_epoch:
            return -1.0  # Signal unknown age
        
        return (time.time() - end_epoch) / 86400.0

    def _get_index_age_days(self, index_name: str) -> float:
        """Get index age in days"""
        try:
            response = self.requests.get(f"{self.base_url}/{index_name}/_settings")
            if response.status_code != 200:
                logger.error(f"Error getting settings for {index_name}: HTTP {response.status_code} - Response: {response.text}")
                return 0
            settings = response.json()
            created_ms = int(settings[index_name]["settings"]["index"]["creation_date"])
            age_seconds = time.time() - (created_ms / 1000)
            return age_seconds / 86400
        except Exception as e:
            logger.debug(f"Error calculating age for {index_name}: {e}")
            return 0

    def _get_searchable_snapshot_age_days(self, searchable_index_name: str) -> float:
        """Get age of searchable snapshot index based on underlying snapshot timestamp"""
        try:
            # Extract snapshot name from searchable index name
            snapshot_name = searchable_index_name.replace('-snapshot', '')
            
            # Find the snapshot in our snapshot list
            for snapshot in self.get_snapshots():
                if snapshot['id'] == snapshot_name:
                    snapshot_age = self._snapshot_age_days(snapshot)
                    if snapshot_age >= 0:
                        logger.debug(f"Searchable index {searchable_index_name} age based on snapshot: {snapshot_age:.1f} days")
                        return snapshot_age
                    break
            
            # Fallback to index creation date if snapshot not found or has invalid age
            logger.debug(f"Could not determine snapshot age for {searchable_index_name}, falling back to index age")
            return self._get_index_age_days(searchable_index_name)
            
        except Exception as e:
            logger.debug(f"Error calculating snapshot-based age for {searchable_index_name}: {e}")
            # Fallback to index creation date
            return self._get_index_age_days(searchable_index_name)

    def _get_indices_by_pattern(self, pattern: str) -> List[Dict[str, Any]]:
        """Get indices matching pattern"""
        try:
            response = self.requests.get(f"{self.base_url}/_cat/indices/{pattern}?format=json")
            if response.status_code == 404:
                # No indices match pattern - this is normal
                logger.debug(f"No indices found matching pattern {pattern}")
                return []
            elif response.status_code != 200:
                logger.error(f"Error getting indices for pattern {pattern}: HTTP {response.status_code} - Response: {response.text}")
                return []
            
            indices = response.json()
            logger.debug(f"API call GET /_cat/indices/{pattern} returned {len(indices)} indices")
            return indices
        except Exception as e:
            logger.error(f"Error getting indices for pattern {pattern}: {e}")
            return []

    def _snapshot_exists(self, snapshot_name: str) -> bool:
        """Check if snapshot exists"""
        try:
            response = self.requests.get(f"{self.base_url}/_snapshot/data/{snapshot_name}")
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Error checking if snapshot {snapshot_name} exists: {e}")
            return False
    
    def _index_exists(self, index_name: str) -> bool:
        """Check if index exists"""
        try:
            response = self.requests.head(f"{self.base_url}/{index_name}")
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Error checking if index {index_name} exists: {e}")
            return False

    def _get_corresponding_snapshot_name(self, index_name: str) -> Optional[str]:
        """Get corresponding snapshot name for an index"""
        # For searchable snapshots (e.g., log-000001-snapshot), find the base snapshot (log-000001)
        if index_name.endswith("-snapshot"):
            base_name = index_name.replace("-snapshot", "")
            return base_name
        
        # For regular indices, the snapshot name is the same as index name
        return index_name

    def _validate_configuration(self) -> None:
        """Validate ILM configuration for common issues"""
        logger.debug("Validating ILM configuration")
        
        if self.hot_storage_days < 0:
            raise ValueError(f"Hot storage days must be >= 0, got {self.hot_storage_days}")
        
        if self.total_retention_days < 0:
            raise ValueError(f"Total retention days must be >= 0, got {self.total_retention_days}")
            
        if self.hot_storage_days > self.total_retention_days:
            raise ValueError(f"Hot storage days ({self.hot_storage_days}) cannot exceed total retention ({self.total_retention_days})")
            
        if self.rollover_size_gb <= 0:
            raise ValueError(f"Rollover size must be > 0, got {self.rollover_size_gb}")
            
        if self.rollover_age_days <= 0:
            raise ValueError(f"Rollover age must be > 0, got {self.rollover_age_days}")
            
        if not self.managed_index_patterns:
            raise ValueError("At least one managed index pattern must be specified")
            
        logger.info(f"ILM configuration validated: hot_storage={self.hot_storage_days}d, total_retention={self.total_retention_days}d, rollover_size={self.rollover_size_gb}GB, rollover_age={self.rollover_age_days}d, patterns={self.managed_index_patterns}")