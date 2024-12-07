"""
DataAgg.py: Class for aggregating data.
"""

# need to do mean, min, max aggregation for all of them, and return aggregation type
# return file name, aggregation type, size, file min, file max, actual sp res, actual temp res
# return OG file in the list as well

class DataAgg:
    self.all_t = [0, 1, 2, 3]   # number of aggregations, corresp. to ['Hour', 'Day', 'Month', 'Year']
    self.all_s = [0, 1, 2]  # number of aggregations, corresp. to [0.25, 0.5, 1.0]
     
    def __init__(self, name, t, target):
        self.name = name    # file name
        self.t = t  # True if temporal agg, False if spatial agg
        self.target = target    # either temporal or spatial resolution

        self.too_fine_list = []
        self.metadata_list = []

	def temporal_agg()
		"""
		def -	temporal_agg: aggregates data up one dimension IN current res, current f_id OUT metadata for file
			(metadata: min, max, file size, file name) <-- call helper functions in utils
			save metadata to metadata_list as a dict: {'var': ,'T':, 'tao':,'S':, 'sigma':, 'min_val':, 'max_val':, 'file_size':, 'file_name'} - leave T, S empty to fill in in main
		"""

	def make_temporal_agg_files():
		"""
		Aggregates data temporally to the highest temporal aggregation.

		Returns:
			metadata_list: List of dictionaries containing metadata for aggregated files.
			too_fine_list: List of file names that have finer temporal aggregation than is needed.
		"""

