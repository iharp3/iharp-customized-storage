"""
DataAgg.py: Class for aggregating data.
"""
import os
import xarray as xr

from utils import get_agg_file_name, get_scale_offset, get_file_size, get_data_path, modify_filename

import config

# need to do mean, min, max aggregation for all of them, and return aggregation type
# return file name, aggregation type, size, file min, file max, actual sp res, actual temp res
# return OG file in the list as well

class DataAgg:
	def __init__(self, name, var, t, target, constant):
		self.name = name    # file name
		self.var = config.VAR_SHORT_N[var]	# variable in file
		self.t = t  # True if temporal agg, False if spatial agg
		if t:
			self.target = str(target)
		else:
			self.target = float(target)
		self.constant = constant	# either temporal or spatial resolution (depends on self.t)
		self.metadata_list = []
		self.all_t = ["1H", "1D", "1M", "1YE"]
		self.all_s = [0.25, 0.5, 1.0]

	def compress_save_and_get_dict(self, agg, name, t_res, s_res, agg_type):
		"""
		Get scale and offset to compress dataset. Save compressed dataset.
		Return a dictionary with dataset metadata.
		"""
		file_path = get_data_path(name)
		scale, offset, v_min, v_max = get_scale_offset(arr=agg[self.var])

		agg.to_netcdf(
			file_path,
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

		d = {"temporal_resolution":t_res,
			 "spatial_resolution":s_res,
			 "agg_type": agg_type,
			 "min":round(v_min, 2),
			 "max":round(v_max, 2),
			 "file_size":get_file_size(file_path),
			 "file_name":name}
		
		return d

	def get_all_agg_names(self):
		"""
		Make names for aggregations
		"""
		agg_name = get_agg_file_name(self.name, t=self.t)

		mean_agg_name = modify_filename(cur_file_name=agg_name, agg_type="_mean.nc")
		min_agg_name = modify_filename(cur_file_name=agg_name, agg_type="_min.nc")
		max_agg_name = modify_filename(cur_file_name=agg_name, agg_type="_max.nc")

		return mean_agg_name, min_agg_name, max_agg_name

	def temporal_agg(self, dataset, resolution):
		"""
		
		"""
		# Get agg file names
		mean_agg_name, min_agg_name, max_agg_name = self.get_all_agg_names()

		# Mean aggregation
		mean_agg = dataset.resample(valid_time=resolution).mean()	# TODO: Might need to persist dataset after this
		d_mean = self.compress_save_and_get_dict(agg=mean_agg, name=mean_agg_name, t_res=resolution[1], s_res=self.constant, agg_type="mean")

		# Min aggregation
		min_agg = dataset.resample(valid_time=resolution).min()	# TODO: Might need to persist dataset here
		d_min = self.compress_save_and_get_dict(agg=min_agg, name=min_agg_name, t_res=resolution[1], s_res=self.constant, agg_type="min")

		# Max aggregation
		max_agg = dataset.resample(valid_time=resolution).max()	# TODO: Might need to persist dataset here
		d_max = self.compress_save_and_get_dict(agg=max_agg, name=max_agg_name, t_res=resolution[1], s_res=self.constant, agg_type="max")
	
		return [d_mean, d_min, d_max]

	def spatial_agg(self, dataset, resolution):
		"""
		"""
		# Get agg file names
		mean_agg_name, min_agg_name, max_agg_name = self.get_all_agg_names()

		# Get coarsen factor
		c_f = int(resolution/config.RAW_SP_RES)

		# Mean aggregation
		mean_agg = dataset.coarsen(latitude=c_f, longitude=c_f, boundary="trim").mean()
		d_mean = self.compress_save_and_get_dict(agg=mean_agg, name=mean_agg_name, t_res=self.constant, s_res=resolution, agg_type="mean")

		# Min aggregation
		min_agg = dataset.coarsen(latitude=c_f, longitude=c_f, boundary="trim").min()
		d_min = self.compress_save_and_get_dict(agg=min_agg, name=min_agg_name, t_res=self.constant, s_res=resolution, agg_type="min")

		# Max aggregation
		max_agg = dataset.coarsen(latitude=c_f, longitude=c_f, boundary="trim").max()
		d_max = self.compress_save_and_get_dict(agg=max_agg, name=max_agg_name, t_res=self.constant, s_res=resolution, agg_type="max")

		return [d_mean, d_min, d_max]

	def make_temporal_agg_files(self):
		"""
		Aggregates data temporally from the finest (hourly) to the coarsest (yearly) temporal aggregation.

		Returns:
			metadata_list: List of dictionaries containing metadata for aggregated files.
		"""
		# Get dataset to aggregate
		file_path = get_data_path(self.name)
		ds = xr.open_dataset(file_path, chunks={config.TIME: config.NUM_CHUNKS})	#TODO: check time dimension has name "valid_time"
		ds = ds.chunk({"valid_time": 8760})	# Re-chunking by the number of hours in a year

		for i in range(self.all_t.index(self.target), len(self.all_t)):	# This for-loop gets us every temporal resolution
			resolution = str(self.all_t[i])
			if resolution == "1H":
				mean_min_max_metadata_list = self.compress_save_and_get_dict(agg=ds, name=self.name, t_res=config.RAW_T_RES, s_res=self.constant, agg_type="none")
			else:
				mean_min_max_metadata_list = self.temporal_agg(ds, resolution)	# This function gets us all agg types (mean, min, max)
			self.metadata_list = self.metadata_list + mean_min_max_metadata_list	# Save dicts to metadata_list

		return self.metadata_list

	def make_spatial_agg_files(self):
		"""
		Aggregates data spatially from the finest (0.25) to the coarsest (1.0) spatial aggregation.

		Returns:
			metadata_list: List of dictionaries containing metadata for aggregated files.
		"""
		# Get dataset to aggregate
		file_path = get_data_path(self.name)
		ds = xr.open_dataset(file_path, chunks='auto')	#TODO: determine good chunks

		for i in range(self.all_s.index(self.target), len(self.all_s)):	# This for-loop gets us every spatial resolution
			resolution = float(self.all_s[i])
			if resolution == 0.25:
				mean_min_max_metadata_list = self.compress_save_and_get_dict(agg=ds, name=self.name, t_res=self.constant, s_res=config.RAW_SP_RES, agg_type="none")
			else:
				mean_min_max_metadata_list = self.spatial_agg(ds, resolution)
			self.metadata_list = self.metadata_list + mean_min_max_metadata_list

		return self.metadata_list
