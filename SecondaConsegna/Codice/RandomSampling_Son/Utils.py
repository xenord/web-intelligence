__author__ = 'Francesco Benetello'

from pyspark import SparkContext, SparkConf
import pyspark


class Utils:
	"""docstring for Utils"""
	MinimunSupport = 0
	DatasetLength = 0
	SubsetLength = 0
	Rdd = 0
	Support = 0
	Sample_size = 0
	Numpartitions = 0
	isSon = False

	def __init__(self, rdd, support, numpartitionsORsample_size, isSon):
		if (isSon == True):
			self.Rdd = rdd
			self.Support = support
			self.Numpartitions = numpartitionsORsample_size
			self.isSon = isSon
		if (isSon == False):
			self.Rdd = rdd
			self.Support = support
			self.Sample_size = numpartitionsORsample_size
			self.isSon = isSon

	def getDatasetLength(self):
		return self.DatasetLength

	def setDatasetLength(self, DatasetLength):
		self.DatasetLength = DatasetLength

	def getMinimunSupport(self):
		return self.MinimunSupport

	def setMinimunSupport(self, MinimunSupport):
		self.MinimunSupport = MinimunSupport

	def getSubsetLength(self):
		return self.SubsetLength

	def setSubsetLength(self, SubsetLength):
		self.SubsetLength = SubsetLength

	def SubsetLength(self):
		self.setSubsetLength((self.getDatasetLength() * self.Sample_size)//100)

	def length_dataset(self):
		lineLengths = self.Rdd.map(lambda s: len(s))
		length = lineLengths.collect()
		ln = 0
		for x in length:
			ln += 1
		self.setDatasetLength(ln)

	def minsupport_for_random_sampling(self):
		### Calcolo supporto minimo e lunghezza del sample
		self.length_dataset()
		subsetLength = (self.getDatasetLength() * self.Sample_size)//100
		self.setMinimunSupport((self.Support*subsetLength)/100)

	def minsupport_for_son(self, support, numpartitions, Dataset_len):
		minsup =  (support*(Dataset_len//numpartitions))/100
		self.setMinimunSupport(minsup)