#from fastavro import reader
import quickavro
import glob
for f in glob.glob("*.avro"):
	print(f)
	with quickavro.FileReader(f) as reader:
		print("%s: %s", (f, reader.header))
		# i = 0
		# for record in reader.records():
			# i = i+1
			# if i % 5000 == 0:
				# print("%s: %s" %(f, i))
	with open(f, 'rb') as fo:
		avro_reader = reader(fo)
		print("%: %",(f, list(avro_reader).count()))