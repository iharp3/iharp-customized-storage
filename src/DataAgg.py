"""
DataAgg.py: Class for aggregating data.
"""
import os
import xarray as xr

from utils import get_agg_file_name, get_scale_offset, get_file_size

# need to do mean, min, max aggregation for all of them, and return aggregation type
# return file name, aggregation type, size, file min, file max, actual sp res, actual temp res
# return OG file in the list as well

class DataAgg:
    # self.all_t = [0, 1, 2, 3]   # number of aggregations, corresp. to ['Hour', 'Day', 'Month', 'Year']
    # self.all_s = [0, 1, 2]  # number of aggregations, corresp. to [0.25, 0.5, 1.0]

	self.all_t = ["1D", "1M", "1Y"]

    def __init__(self, name, var, t, target):
        self.name = name    # file name
		self.var = var	# variable in file
        self.t = t  # True if temporal agg, False if spatial agg
        self.target = target    # either temporal or spatial resolution <-- for deleting coarser files

        self.too_fine_list = []
        self.metadata_list = []

	def temporal_agg(dataset, resolution):
		"""
		def -	temporal_agg: aggregates data up one dimension IN current res, current f_id OUT metadata for file
		"""
		# Make names for aggregations
		agg_name = get_agg_file_name(self.name, t=self.t)

		mean_agg_name = modify_filename(cur_file_name=agg_name, agg_type="_mean.nc")
		min_agg_name = modify_filename(cur_file_name=agg_name, agg_type="_min.nc")
		max_agg_name = modify_filename(cur_file_name=agg_name, agg_type="_max.nc")

		# Mean aggregation
		mean_agg = dataset.resample(time=resolution).mean()
				# TODO: Might need to persist dataset here
		scale, offset, v_min, v_max = get_scale_offset(arr=mean_agg[self.var])
		mean_agg.to_netcdf(
			get_data_path(mean_agg_name),
			encoding={
				self.var: {
					"dtype": "int16",
					"missing_value": -32767,
                	"_FillValue": -32767,
                	"scale_factor": scale,
                	"add_offset": offset,
				}
			}
		)
		mean_metadata = {"temporal_resolution":resolution[1],
						 "spatial_resolution":0.25, 
						 "min":v_min, 
						 "max":v_max, 
						 "file_size":get_file_size(get_data_path(mean_agg_name)), 
						 "file_name":mean_agg_name}

		# Compute and save files to netcdf

	
	def spatial_agg(resolution):
		"""
		Performs one temporal aggregation. 
			(metadata: min, max, file size, file name) <-- call helper functions in utils
		"""

	def make_temporal_agg_files():
		"""
		Aggregates data temporally from the finest (hourly) to the coarsest (yearly) temporal aggregation.

		Returns:
			metadata_list: List of dictionaries containing metadata for aggregated files.
			too_fine_list: List of file names that have finer temporal aggregation than is needed.

		dict: {'var': ,'T':, 'tao':,'S':, 'sigma':, 'min_val':, 'max_val':, 'file_size':, 'file_name'} - leave T, S empty to fill in in main
		
# Daily averages
daily_avg = ds.resample(time="1D").mean()
daily_avg.to_netcdf("daily_avg.nc")

# Monthly averages
monthly_avg = ds.resample(time="1M").mean()
monthly_avg.to_netcdf("monthly_avg.nc")

# Yearly averages
yearly_avg = ds.resample(time="1Y").mean()
yearly_avg.to_netcdf("yearly_avg.nc")
		
		"""
		# Get file path
		file_path = get_data_path(self.name)
		# Open file
		ds = xr.open_dataset(file_path, chunks={"valid_time": 1000})	#TODO: check time dimension has name "valid_time"

		# Get all temporal aggregations (mean, max, min)
		for resolution in self.all_t:
			agg_metadata = self.temporal_agg(ds, resolution)

		# add file name to too_fine OR add dict to metadata_list

