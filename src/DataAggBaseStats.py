"""
DataAggBaseStats.py: Class for aggregating data. Only gets mim_min, max_max, mean_mean stat combos of resolutions
"""
import os
import xarray as xr

from utils import get_agg_file_name, get_min_max_of_array, get_file_size, get_data_path, modify_filename_unique

import config

# need to do mean, min, max aggregation for all of them, and return aggregation type
# return file name, aggregation type, size, file min, file max, actual sp res, actual temp res
# return OG file in the list as well

class DataAgg:
	def __init__(self, name, var, t, target, constant):
		self.name = name    # file name
		self.cur_name = name	# updated file name
		self.var = config.VAR_SHORT_N[var]	# variable in file
		self.t = t  # True if temporal agg, False if spatial agg
		if t:
			self.target = str(target)
		else:
			self.target = float(target)
		self.temp_agg_type = ""
		self.constant = constant	# either temporal or spatial resolution (depends on self.t)
		self.metadata_list = []
		self.all_t = ["1H", "1D", "1ME", "1YE"]
		self.all_s = [0.25, 0.5, 1.0]

	def compress_save_and_get_dict(self, agg, name, t_res, s_res, agg_type, e, c=True):
		"""
		Get scale and offset to compress dataset. Drop unwanted dimensions from dataset. Save compressed dataset.
		Return a dictionary with dataset metadata.
		"""
		# drop unwanted dimensions, rename dim
		# agg = agg.rename({"valid_time": "time"})
		if c:
			if "number" in agg.coords:
				agg = agg.drop_vars("number")
			if "expver" in agg.coords:
				agg = agg.drop_vars("expver")

		v_min, v_max = get_min_max_of_array(arr=agg[self.var])
		file_path = get_data_path(name)

		if c:	# case where file is new and needs to be saved
			agg[self.var].encoding = e	# new encoding 
			agg.to_netcdf(file_path)

		if self.t:	# temporal agg
			d = {"temporal_resolution":t_res,
				"spatial_resolution":s_res,
				"temporal_agg_type": agg_type,
				"spatial_agg_type": "none",
				"min":round(v_min, 2),
				"max":round(v_max, 2),
				"file_size":get_file_size(file_path),
				"file_name":name}
		else:	# spatial agg
			d = {"temporal_resolution":t_res,
			 "spatial_resolution":s_res,
			 "temporal_agg_type": self.temp_agg_type,
			 "spatial_agg_type": agg_type,
			 "min":round(v_min, 2),
			 "max":round(v_max, 2),
			 "file_size":get_file_size(file_path),
			 "file_name":name}
		
		return d

	def spatial_agg(self, dataset, resolution):
		"""
		"""
		cur_encoding = dataset[self.var].encoding

		# Get coarsen factor
		c_f = int(resolution/config.RAW_SP_RES)

		if self.temp_agg_type == 'mean':
			mean_agg_name = modify_filename_unique(cur_file_name=self.cur_name, agg_type="_mean.nc")
			# Mean aggregation
			mean_agg = dataset.coarsen(latitude=c_f, longitude=c_f, boundary="trim").mean()
			d_m = self.compress_save_and_get_dict(agg=mean_agg, name=mean_agg_name, t_res=self.constant, s_res=resolution, agg_type="mean", e=cur_encoding, c=True)

		elif self.temp_agg_type == 'min':
			min_agg_name = modify_filename_unique(cur_file_name=self.cur_name, agg_type="_min.nc")
			# Min aggregation
			min_agg = dataset.coarsen(latitude=c_f, longitude=c_f, boundary="trim").min()
			d_m = self.compress_save_and_get_dict(agg=min_agg, name=min_agg_name, t_res=self.constant, s_res=resolution, agg_type="min", e=cur_encoding, c=True)

		elif self.temp_agg_type == 'max':
			max_agg_name = modify_filename_unique(cur_file_name=self.cur_name, agg_type="_max.nc")
			# Max aggregation
			max_agg = dataset.coarsen(latitude=c_f, longitude=c_f, boundary="trim").max()
			d_m = self.compress_save_and_get_dict(agg=max_agg, name=max_agg_name, t_res=self.constant, s_res=resolution, agg_type="max", e=cur_encoding, c=True)

		else:
			print(f'\tTemporal aggregation type {self.temp_agg_type} (type{type(self.temp_agg_type)}) unknown.')
			d_m = {}

		return [d_m]	

	def make_spatial_agg_files(self, cur_t_agg_type):
		"""
		Aggregates data spatially from the finest (0.25) to the coarsest (1.0) spatial aggregation.

		Returns:
			metadata_list: List of dictionaries containing metadata for aggregated files.
		"""
		# List to save files to delete
		self.temp_agg_type = cur_t_agg_type
		# Get dataset to aggregate
		file_path = get_data_path(self.name)
		ds = xr.open_dataset(file_path, chunks='auto')	#TODO: determine good chunks

		for i in range(self.all_s.index(self.target), len(self.all_s)):	# This for-loop gets us every spatial resolution
			print(f"\t\tSpatial agg \t{i}")
			resolution = float(self.all_s[i])
			if resolution == 0.25:
				cur_encoding = ds[self.var].encoding
				mean_min_max_metadata_list = self.compress_save_and_get_dict(agg=ds, name=self.name, t_res=self.constant, s_res=config.RAW_SP_RES, agg_type="none", e=cur_encoding, c=False)
			else:
				mean_min_max_metadata_list = self.spatial_agg(ds, resolution)
				
			if isinstance(mean_min_max_metadata_list, dict):
				# print(mean_min_max_metadata_list)
				mean_min_max_metadata_list = [mean_min_max_metadata_list]
			elif isinstance(mean_min_max_metadata_list, list):
				mean_min_max_metadata_list = mean_min_max_metadata_list
			else:
				raise TypeError(f"Object {mean_min_max_metadata_list} is neither a dictionary nor a list\ntype {type(mean_min_max_metadata_list)}")

			self.metadata_list = self.metadata_list + mean_min_max_metadata_list

		print('about to return from make_spatial_agg_files')
		return self.metadata_list
