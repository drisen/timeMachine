#
# Copyright 2020 Dennis Risen, Case Western Reserve University
#
from timeMachine import *

""" Test timeMachine operation"""

# initialize the times for far past, 4 polls, far in the future
if False:								# to use epoch msec
	from mylib import strfTime, strpTime
	t0 = int(1000*strpTime('2020-05-06', '%Y-%m-%d'))  # msecs
	delta_t = 24*60*60*1000					# milliseconds in a day
else:									# instead to use small integers for times
	def strfTime(msec: int) -> str:
		return str(msec)

	t0 = int(1000)
	delta_t = 1000

polls = [t0+i*delta_t for i in range(4)]
print(f"Daily poll times are: {', '.join([strfTime(t) for t in polls])}")
polls.insert(0, 0)						# time far in the past
polls.append(t0+1000*delta_t)			# time far in the future

# [name, [0=not_present, 1=value1, 2=value2]+]
entities = [
	['i', 1, 2, 2, 2],
	['h', 1, 1, 2, 2],
	['g', 1, 1, 1, 2],
	['f', 1, None, 1, 1],
	['e', 1, None, 2, 2],
	['d', 1, None, None, 2],
	['c', 1, None, None, 2],
	['b', 1, 1, 1, None],
	['a', None, 1, 1, None]
]


def dif_print(a: list, b: list, a_title: str, b_title: str, fmt: str):
	"""Compare and report the differences between a and b

	Parameters:
		a:			list of records. sorted by record[0]
		b:			list of records. sorted by record[0]
		a_title:	title string for a in left column
		b_title:	title string for b in right
		fmt:		format string for printing a record
	"""
	if a == b:  # lists equal?
		return True
	op = '==' if len(a) == len(b) else '!='
	print(f"\nt={strfTime(t)} len(values)={len(a)} {op} {len(b)}=len(expected)")
	print(format(a_title, fmt), format(b_title, fmt))

	a_rec = a.pop(0) if len(a) > 0 else None
	b_rec = b.pop(0) if len(b) > 0 else None
	while a_rec is not None or b_rec is not None:
		if a_rec is None:  # out of a records?
			print(format('', fmt), format(str(b_rec), fmt))  # yes print b record
			b_rec = b.pop(0) if len(b) > 0 else None
		elif b_rec is None:  # out of ent_list records?
			print(format(str(a_rec), fmt))  # yes print ent_list record
			a_rec = a.pop(0) if len(a) > 0 else None
		# record present in both streams
		elif a_rec == b_rec:  # records equal?
			a_rec = a.pop(0) if len(a) > 0 else None  # get next records and ignore
			b_rec = b.pop(0) if len(b) > 0 else None
		elif a_rec[0] == b_rec[0]:  # No. primary keys equal?
			print(format(str(a_rec), fmt), format(str(b_rec), fmt))  # print both
			a_rec = a.pop(0) if len(a) > 0 else None
			b_rec = b.pop(0) if len(b) > 0 else None
		elif a_rec[0] < b_rec[0]:  # record in a but not b?
			print(format(str(a_rec), fmt))  # print a
			a_rec = a.pop(0) if len(a) > 0 else None
		else:  # record in b but not in a
			print(format('', fmt), format(str(b_rec), fmt))  # print b record
			b_rec = b.pop(0) if len(b) > 0 else None
	return False


def entity(ind: int, msec: int, loose: int) -> dict:
	"""Return the status of entity 'ind' at time 'msec'

	Parameters::
		ind:		index into entities list
		msec:		polledTime in msec
		loose:		0: only status at msec: -1: also immediately previous status;
					+1: also immediately following status
	Returns:
		entity record for entity 'ind' at time 'msec', or None if not present
	"""

	def me_entry(indx: int) -> dict:
		return {'id': ind, 'name': me[0], 'val': me[indx], 'other': 'constant'}

	me = entities[ind]					# the entity
	d2 = int(delta_t/2)
	# find index into polls such that polls[i]-delta_t/2 <= msec < polls[i+1]-delta_t/2
	prev = None							# greatest poll index where me is present
	for i in range(len(polls)-1):
		if polls[i]-d2 <= msec < polls[i+1]-d2:
			break
		if isinstance(me[i], int):		# defined at this poll time?
			prev = i					# Yes. Remember the last poll index
	else:
		print('internal error')
		raise KeyError
	if i == 0 and loose < 1:  			# msec before the 1st poll?
		return None  					# Yes. Entity is not defined
	if i > 0 and me[i] is not None:  	# entity defined at msec?
		return me_entry(i)				# Yes. Return poll record
	if loose == 0:						# No. Entity must be present at time=msec?
		return None						# Yes. No record
	elif loose < 0:						# Allowed to return previous record?
		if prev is not None:			# Yes. Found a previous record?
			return me_entry(prev)		# Yes. return it
		return None						# No. No record
	for j in range(i+1, len(polls)-1): 	# find next poll where present
		if me[j] is not None:			# present in poll[j]?
			return me_entry(j)			# Yes. return it
	return None							# did not find a poll


def verify(tm: TimeMachine, ti: TableIndex):
	print(f"For each 1/4 poll period in time, verify\n\
	TimeMachine.values() == loaded values\n\
	TimeMachine.get(key, None) == loaded value == TableIndex.get(name, None)")
	for loose in [0, -1, 1]:
		tm.loose = loose
		print(f"loose={loose}")
		for t in range(t0-delta_t, t0+(len(polls)-1)*delta_t, int(delta_t/4)):
			tm.epoch_msec = t
			hdr = False
			for k in range(len(entities)):  # for each primary key value
				a = tm.get(k, None)			# value via tm primary key
				b = entity(ind=k, msec=t, loose=loose)  # expected value
				c = ti.get(entities[k][0], None)  # value via alternate key
				if a != b:
					if not hdr:
						print(f"  t={strfTime(t)}")
						hdr = True
					print(f"{str(a):58}!={str(b)}")
				if c != b:
					if not hdr:
						print(f"  t={strfTime(t)}")
						hdr = True
					print(f"{str(c):58}!={str(b)} via name")
			tm_list = [(k, v) for k, v in tm.items()]
			ent_list = []					# list of tuple for each record
			for i in range(len(entities)):
				v = entity(ind=i, msec=t, loose=loose)
				if v is not None:
					ent_list.append((v[key], v))  # key, value
			tm_list.sort(key=lambda x: x[0])  # sort each by primary key
			ent_list.sort(key=lambda x: x[0])
			dif_print(tm_list, ent_list, 'TimeMachine', 'Expected', fmt)


# create the TimeMachine
tm = TimeMachine('test', key_source="lambda x: x['id']")
# load it with test records
print('Loading TimeMachine with daily poll')
tm.update([],backup_poll_msec=t0-delta_t)  # 0th poll delta_t before 1st poll
for i in range(1, len(polls)-1):		# for each polledTime
	t = polls[i]						# the polledTime
	poll = []							# build the list of records in this poll
	for j in range(len(entities)):		# for each entity
		rec = entity(ind=j, msec=t, loose=0)
		if rec is not None:
			poll.append(rec)
	tm.update(poll=poll, backup_poll_msec=t)
	print(f"loaded t={strfTime(t)}: {poll}")
	if i == 1:							# the first poll?
		first_poll = poll				# Yes. Save it for later

key = 'id'
fmt = '60'
print('loading out of order poll that should be rejected')
print(first_poll, polls[1])
tm.update(first_poll, backup_poll_msec=polls[0])
print("Creating index after the updates")
tm_name = TableIndex(tm, key_source="lambda x: x['name']")  # additional index
print('\nTable statistics')
tm_stats = tm.statistics(verbose=1)
print(tm_stats)							# print table statistics
verify(tm, tm_name)						# verify initial TimeMachine

print("Dumping TimeMachine")
tm.dump_gz(filename='test.json.gz')
print("Creating TimeMachine with additional index")
loaded = TimeMachine('test')
print("Loading into new TimeMachine")

loaded_name = TableIndex(loaded, key_source="lambda x: x['name']")  # additional index
loaded.load_gz(filename='test.json.gz')
print("Verifying the loaded TimeMachine")
loaded_stats = loaded.statistics(verbose=1)
if tm_stats == loaded_stats:
	print("statistics are the same as original")
else:
	print(f"statistics are different. Statistics:\n{loaded_stats}")
verify(loaded, loaded_name)


