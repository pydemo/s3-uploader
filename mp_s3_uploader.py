"""#############################################################################
#File-to-S3 Data Loader (v1.2, beta, 04/05/2016 15:11:53) [64bit] 
#Copyright (c): 2016 Alex Buzunov, Free to use, change, or distribute. #FreeUkraine #StopRussia
#Agreement: Use this tool at your own risk. Author is not liable for any damages 
#           or losses related to the use of this software.
################################################################################
Usage:  
#---------------------------------------------------------------------- 
#FreeUkraine #SaveUkraine #StopRussia #PutinKhuilo #CrimeaIsUkraine
#----------------------------------------------------------------------
  set AWS_ACCESS_KEY_ID=<you access key>
  set AWS_SECRET_ACCESS_KEY=<you secret key>
  s3_percent_uploader.exe <file_to_transfer> <bucket_name> [<s3_key_name>] [<use_rr>] [<public>]
	if <s3_key_name> is not specified, the filename will be used.
	--use_rr -- Use reduced redundancy storage.
	--public -- Make uploaded files public.
	
	Boto S3 docs: http://boto.cloudhackers.com/en/latest/ref/s3.html
"""
import os, sys, time, imp
import logging
from pprint import pprint
import multiprocessing
from optparse import OptionParser
class ImproperlyConfigured(Exception):
    """Base class for Boto exceptions in this module."""
    pass
	
try:
	import boto
	from boto.s3.key import Key
except ImportError:
	raise ImproperlyConfigured("Could not load Boto's S3 bindings")

	
job_status={}

def create_symlink(from_dir, to_dir):
	if (os.name == "posix"):
		os.symlink(from_dir, to_dir)
	elif (os.name == "nt"):
		print (4,from_dir)
		os.system('mklink /J %s %s' % (to_dir, from_dir))
	else:
		log.error('Cannot create symlink. Unknown OS.', extra=d)
def unlink(dirname):
	if (os.name == "posix"):
		os.unlink(dirname)
	elif (os.name == "nt"):
		os.rmdir( dirname )
	else:
		log.error('Cannot unlink. Unknown OS.', extra=d)
e=sys.exit
total_size=0
DEBUG =1 
jobname='ora_data_spooler'
ts=time.strftime('%Y%m%d_%a_%H%M%S') #timestamp
max_pool_size=multiprocessing.cpu_count() * 2
HOME= os.path.dirname(os.path.abspath(__file__))

config_home = os.path.join(HOME,'config')
#output------------------------------------
latest_out_dir =os.path.join(HOME,'output','latest')
ts_out_dir=os.path.join(HOME,'output',ts)
latest_dir =os.path.join(HOME,'log','latest')
ts_dir=os.path.join(HOME,'log',ts)

done_file= os.path.join(ts_dir,'DONE.txt')
job_status_file=os.path.join(ts_dir,'%s.%s.status.py' % (os.path.splitext(__file__)[0],jobname))	

d = {'iteration': 0, 'pid':0, 'rows':0}
FORMAT = '|%(asctime)-15s|%(pid)-5s|%(iteration)-2s|%(rows)-9s|%(message)-s'
if not DEBUG:	
	logging.basicConfig(filename=os.path.join(ts_dir,'%s.log' % jobname),level=logging.DEBUG,format=FORMAT)
else:
	logging.basicConfig(level=logging.DEBUG,format=FORMAT)

log = logging.getLogger(jobname)

def import_module(filepath):
	class_inst = None
	mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])
	assert os.path.isfile(filepath), 'File %s does not exists.' % filepath
	if file_ext.lower() == '.py':
		py_mod = imp.load_source(mod_name, filepath)

	elif file_ext.lower() == '.pyc':
		py_mod = imp.load_compiled(mod_name, filepath)
	return py_mod	
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
assert AWS_SECRET_ACCESS_KEY, 'AWS_SECRET_ACCESS_KEY is not set'
assert AWS_ACCESS_KEY_ID, 'AWS_ACCESS_KEY_ID is not set'
#e(0)
def progress(complete, total):
	sys.stdout.write('Uploaded %s bytes of %s (%s%%)\n' % (complete, total, int(100*complete/total)))

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)	
	
def upload_file_to_s3(data):
	#global bucket
	id, file, opt=data	
	transfer_file, bucket_name, s3_key_name, use_rr, make_public = file
	# open the wikipedia file
	if not s3_key_name:
		s3_key_name = os.path.basename(transfer_file)
	
	
	sys.stdout.write('Connecting to S3...\n')
	conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
	bucket = conn.get_bucket(bucket_name)
	#pprint(dir(bucket))
	#print 
	#e(0)
	file_handle = open(transfer_file, 'rb')

	k = Key(bucket)
	k.key = s3_key_name
	#pprint(dir(k))
	#e(0)
	#sys.stdout.write('Uploading to '+bucket.get_website_endpoint()+'...\n')
	sys.stdout.write('File size: %s\n' % sizeof_fmt(os.path.getsize(transfer_file)))
	sys.stdout.write('Public = %s\n' % str(make_public))
	sys.stdout.write('ReducedRedundancy = %s\n' % str(use_rr))
	
	
	k.set_contents_from_file(file_handle, cb=progress, num_cb=20, reduced_redundancy=use_rr )
	if make_public:
		k.make_public()

	sys.stdout.write('Upload complete.\n')

	return '/'.join((bucket_name, s3_key_name))

def get_transfer_files(config,v):
	dir_set=config.cfg['input_dir_sets'][v['from_dir_set']]
	for dir_filter in dir_set.split(os.linesep):
		print dir_filter
		files=glob.glob(dir_filter)
		print files
		
		
	e(0)
if __name__ == "__main__":
	parser = OptionParser()
	parser.add_option("-c", "--spool_config", dest="spool_config", type=str, default='s3upload_config.py')
	parser.add_option("-p", "--pool_size", dest="pool_size", type=int, default=multiprocessing.cpu_count() * 2)

	parser.add_option("-r", "--use_rr", dest="use_rr", action="store_true", default=False)
	parser.add_option("-u", "--make_public", dest="make_public", action="store_true", default=False)
	
	(opt, args) = parser.parse_args()
	if 0:
		if len(args) < 2:
			print (__doc__)
			sys.exit()
		kwargs = dict(use_rr=options.use_rr, make_public=options.make_public)
		import time
		start_time = time.time()
		location=upload_file_to_s3(*args, **kwargs)
		#print (location)
		
		if options.make_public and location :
			_,region,aws,com =bucket.get_website_endpoint().split('.')		
			#print(region)
			sys.stdout.write('Your file is at: https://%s.%s.%s/%s\n' % (region.replace('-website-','-'),aws,com,location))
		print()
		print("Time elapsed: %s seconds" % (time.time() - start_time))
	
	#
	#mp stuff
	#
	
	config_file = os.path.join(config_home,opt.spool_config)

	if opt.pool_size> max_pool_size:	
		pool_size=max_pool_size
		log.warn('pool_size value is too high. Setting to %d (cpu_count() * 2)' % max_pool_size)
	else:
		pool_size=opt.pool_size

	files=[]
	

	
	#__builtin__.trd_date = None
			
	config=import_module(config_file)
	
	
	
	for k,v in config.cfg['s3_upload'].items():

		transfer_files=get_transfer_files(config,v)
		bucket_name=config.cfg['buckets'][v['to_bucket']]
		sub_bucket=k
		s3_key_name= None
		use_rr= v['use_rr']
		make_public = v['make_public']
		
		
		files.append([conn,fn,q,nls])
		
	m= multiprocessing.Manager()
	if pool_size>len(files):
		pool_size=len(files)
	inputs = list([(i,q, opt) for i,q in enumerate(files)])

	pool = m.Pool(processes=pool_size,
								initializer=start_process,
								)
	pool_outputs = pool.map(upload_file_to_s3, inputs)
	pool.close() # no more tasks
	pool.join()  # wrap up current tasks

	#print ('Pool    :', pool_outputs)
	#e(0)
	print  ('Total rows extracted    : %d' % sum([r[0] for r in pool_outputs]))
	job_status={'spool_status':[r[2] for r in pool_outputs],'spool_files':[r[1] for r in pool_outputs]}
	print ('#'*60)
	for r in pool_outputs:
		print (r[2],r[1])
	print ('#'*60)
	
	atexit.register(save_status)	
	
	

