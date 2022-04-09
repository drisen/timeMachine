#
# Copyright 2020 Dennis Risen, Case Western Reserve University
#
"""
Manage a TimeMachine object
"""
import bisect
import gzip
import json
from mylib import Dict, logErr, strfTime
import os
from time import time

""" TO DO
- Write TimeMachine.back_propagate(field_names: list) method to propagate newly introduced values into older records 
    for key in table

- Write checker for Historical* to:
    verify ascending @id or drop and report repeated id
    report missing id sequences
    number of records in each collection time
    histogram of time between collections, for each month
    added/dropped fields
    (count,value) for each field
"""


def avg(x: int, y: int) -> int:
    """Average with missing data and mixed types. avg(x,None)==avg(None,x)==x"""

    if not isinstance(x, int) and not isinstance(x, float):
        return y
    elif not isinstance(y, int) and not isinstance(y, float):
        return x
    else:
        return int((x+y)/2) if isinstance(x, int) and isinstance(y, int) else (x+y)/2


def str2msec(s: str) -> int:
    """Scan string as integer epoch msec or float epoch seconds"""
    try:
        return int(s)					# assume integer epoch msec
    except ValueError: 					# s is not an int
        try:
            return int(1000*float(s)) 	# assume float epoch seconds
        except ValueError:				# s is not a float either
            return None


class TableIndex:
    def __init__(self, table, key_source: str = None, **kwargs):
        """Initialize a TableIndex object. key_source MUST be equivalent to table's key_source

        Parameters:
            table (TimeMachine):		# The TimeMachine being indexed
            key_source (str): 			# key_func source code. e.g. lambda x: x['id']
        """
        self.table: TimeMachine = table
        self.key_source = key_source
        self.key_func = None if key_source is None else eval(key_source)
        self.d = dict()	 # {key_val: [[first: int, next: int, last: int, rec: dict], ...]
        table.indices.append(self) 		# Add self to list of table's other indices
        for window in table.all_windows():
            self._insert(window)
        super().__init__(**kwargs)

    def __contains__(self, key): 		# OK
        try:
            self.__getitem__(key)
            return True
        except KeyError:
            return False

# 	def __del__(self):					# ***** this is definitely wrong
# 		indices = self.table.indices
# 		del indices[indices.index(self)]  # remove my TableIndex entry in TimeMachine

    def __getitem__(self, item): 		# OK
        return self.find(item, self.table.epoch_msec, self.table.loose)

    def __iter__(self):					# OK
        for x in self.d:
            try:
                self.__getitem__(x)
                yield x
            except ValueError:			# no data for this key at epoch_msec
                continue

    def __str__(self):					# OK
        return f"TI({self.key_source} on {self.table})"

    def get(self, item, default):
        try:
            return self.find(item, self.table.epoch_msec, self.table.loose)
        except (ValueError, KeyError):
            return default

    def items(self):
        for x in self.d:
            try:
                yield x, self.__getitem__(x)
            except ValueError:			# no data for this key at epoch_msec
                continue

    def values(self):
        for x in self.d:
            try:
                yield self.__getitem__(x)
            except ValueError:			# no data for this key at epoch_msec
                continue

    def find(self, key, epoch_msec: int, loose: int) -> dict: 	# OK
        """Return the record for item with primary key=key at time of epoch_msec.

        Parameters:
            key (hashable):		key value for item to find
            epoch_msec (int):	epoch milliseconds
            loose (int):		0 to return only data for epoch_msec inclusive in time window
                        -1 or +1 to prefer data for earlier or later when epoch_msec is not inclusive in a window
        Returns:
            (dict):				record for item at time=epoch_msec
            KeyError 			if no item with key satisfying key
            ValueError			if item is not defined at time=epoch_msec
        """
        item = self.d[key]  # [[first: int, next: int, last: int, rec: dict], ...]
        i = bisect.bisect_right(item, [epoch_msec, TimeMachine.infinity, TimeMachine.infinity])
        if i:
            window = item[i-1]
            if epoch_msec < window[1] or loose == -1:
                return window[3] 		# correct or less match
            elif loose == 1 and i < len(item):
                return item[i][3] 		# greater match
            raise ValueError(f"No data for {key} at {epoch_msec} in {item[i - 1][:3]}")
        else:
            if loose == 1 and i < len(item):
                return item[i][3] 		# greater match
            raise ValueError(f"No data for {key} at {epoch_msec} in {item[i][:3]}")

    def _insert(self, window: dict):
        """insert window into this TableIndex"""
        key = self.key_func(window[3])
        windows = self.d.get(key, None)
        if windows is None:
            self.d[key] = [window]
        else:
            windows.append(window)


class TimeMachine(TableIndex):
    """Stores a sequence of {epoch_msec, item, {attribute, ...} as {item, [[time-window, {field, ...}], ...]}
    Useful for compressing item data that rarely changes with time into memory-resident object
    """
    infinity = 2**42					# epoch milli-seconds far in the future
    dir_path = 'C:/Users/dar5/Google Drive/Case/PyCharm/awsstuff/prod'  # path to timeMachines

    def __init__(self, table_name: str, **kwargs):
        self.indices: list = []  		# [TableIndex, ...] other than the primary index
        self.version: int = 2			# current object version
        self.table_name = table_name 	# name of this table
        self.poll_msec: int = None 		# last poll time included in data
        # records in each list are sorted in ascending order by key_func
        # self.table = dict() 	# {key: [[first: int, next: int, last: int, rec: dict], ...], ...}
        self.msecs = list()				# [msec: int, ...] sorted in ascending order
        self.index: int = None			# index in msecs corresponding to epoch_msec
        self.epoch_msec = int(1000*time())  # default epoch_msec is now
        self.loose = 1					# default is allow match for time <= epoch_msec
        super().__init__(self, **kwargs)

    def __str__(self) -> str:
        """Return printable table statistics"""
        tot = sum([len(windows) for windows in self.d.values()])
        return f"TM({self.indices[0].key_source} on {self.table_name}, last poll_msec={strfTime(self.poll_msec)}, {len(self.d)} entries, {tot} windows)"

    # This works only if the added key is equivalent to the primary key.
    # It won't work, e.g. when primary key is mac and the added key is name, and the 'name' has been
    # assigned to different pieces of equipment over time.
    # Need to deliver windows in chronological order. I.e. merge from all windows
    def all_windows(self):
        for windows in self.d.values():
            for window in windows:
                yield window

    def back_propagate(self, field_names: list, ok_to_copy: callable):
        """Propagate values of newly created/populated fields into older records for the same primary id.

        Parameters:
            field_names (list):	field to copy. [field_name, ...]
            ok_to_copy (callable)	ok_to_copy(new_rec, previous_rec) -> bool:

        """
        """
        For each primary key value starting with its most recent record,
        if op_to_copy(newer_rec, previous_rec), then copy each field in field_names
        from newer_rec to older_rec. Repeat for the primary key until op_to_copy
        returns False.
    
        """
        for key, windows in self.d:
            # windows is [[first, next, last, {a:v, ...}], ...]
            new_rec = windows[-1][3]
            for i in range(len(windows)-1, 0, -1):
                new_rec = windows[i, 3]
                previous_rec = windows[i-1][3]
                if not ok_to_copy(new_rec, previous_rec): 	# should copy occur?
                    break			# No. stop copying for this primary key value
                for field_name in field_names:  # for each field
                    previous_rec[field_name] = new_rec[field_name]  # copy new -> old

    def dump_gz(self, **kwargs): 	# OK
        """Write json-encoded time_window data representation to filename or fp."""
        if 'filename' not in kwargs and 'fp not in kwargs':  # sink missing?
            kwargs['filename'] = os.path.join(TimeMachine.dir_path, self.table_name+'.json.gz')
        with gzip.open(mode='wt', **kwargs) as zip_stream:
            json.dump(self._dump_(), zip_stream)

    def dumps(self) -> str: 	# OK
        """Encode json-encoded time_window data representation."""
        return json.dumps(self._dump_())

    def _dump_(self) -> dict: 	# OK
        """The json-encoded time_window representation"""
        x = {'version': self.version, 'table_name': self.table_name,
            'poll_msec': self.poll_msec, 'd': self.d}
        if self.key_source is not None:
            x['key_source'] = self.key_source
        return x

    def dump_times_gz(self, **kwargs): 	# OK
        """Write json-encoded poll_times list to filename or fp"""
        if 'filename' not in kwargs and 'fp not in kwargs':  # sink missing?
            kwargs['filename'] = os.path.join(TimeMachine.dir_path, self.table_name+'_times.json.gz')
        with gzip.open(mode='wt', **kwargs) as zip_stream:
            json.dump(self._dump_times_(), zip_stream)

    def dumps_times(self) -> str: 	# OK
        """Encode json-encoded poll_times list."""
        return json.dumps(self._dump_times_())

    def _dump_times_(self) -> dict: 	# OK
        return {'version': self.version, 'table_name': self.table_name, 'msecs': self.msecs}

    def loads(self, encoded: str): 	# OK
        """Load self from json-encoded TimeMachine string."""
        self._load_(json.loads(encoded))

    def load_gz(self, **kwargs): 	# OK
        """Load self from json-encoded TimeMachine fp or filename"""
        if 'filename' not in kwargs and 'fp not in kwargs':  # sink missing?
            kwargs['filename'] = os.path.join(TimeMachine.dir_path, self.table_name+'.json.gz')
        with gzip.open(mode='rt', **kwargs) as zs:
            self._load_(json.load(zs))

    def _load_(self, obj: dict):		# OK
        if self.table_name is None:		# table_name is undefined?
            self.table_name = obj['table_name']  # Yes, fill-in from load
        elif self.table_name != obj['table_name']:
            raise ValueError(f"table_name mismatch. {obj['table_name']} != {self.table_name}")
        if self.key_source is None and 'key_source' in obj:  # self doesn't have the code, but the load does?
            self.key_source = obj['key_source']  # get key_source from the load, and compile to key_func
            self.key_func = eval(self.key_source)
        try:
            self.poll_msec = obj['poll_msec']
        except KeyError:
            self.poll_msec = obj['poll_time']
        # json encoding has re-cast the key to str. Recompute keys
        tmp = obj['d']
        self.d = {}
        for windows in tmp.values():
            key = self.key_func(windows[0][3])
            self.d[key] = windows

        for windows in self.d.values():
            for window in windows:
                self.add_window(window)  # update other indices
        if obj['version'] in {1, 2}:
            pass
        else:
            raise ValueError(f"Unknown version={obj['version']}")

    def loads_times(self, encoded: str):
        """Load the json-encoded list of poll_msec into self."""
        self._load_times_(json.loads(encoded))

    def load_times_gz(self, **kwargs):
        if 'filename' not in kwargs and 'fp not in kwargs':  # sink missing?
            kwargs['filename'] = os.path.join(TimeMachine.dir_path, self.table_name+'_times.json.gz')
        with gzip.open(mode='rt', **kwargs) as zs:
            self._load_times_(json.load(zs))

    def _load_times_(self, obj: dict):
        if obj['version'] in {1, 2}:
            if obj['table_name'] != self.table_name:
                raise ValueError(f"table_name mismatch. {obj['table_name']} != {self.table_name}")
            if self.poll_msec is not None and self.poll_msec != obj['msecs'][-1]:
                raise ValueError(f"poll_msec don't match")
            try:
                self.msecs = obj['msecs']
            except KeyError:
                self.msecs = obj['times']
        else:
            raise ValueError(f"Unknown version={obj['version']}")

    def max_msec(self):
        """Return maximum poll_msec in the database"""
        return self.poll_msec

    def min_msec(self):
        """Return minimum poll_msec in the database"""
        if len(self.msecs) > 0:
            return self.msecs[0]
        d = self.indices[0].d
        return min([windows[0][0] for windows in d.values()])

    def set_epoch_msec(self, epoch_msec: int, loose: int = None) -> bool:
        """Sets the default epoch_msec and matching criteria for the table"""
        """
        if len(self.msecs) > 0:			# keeping track of poll times?
            i = bisect.bisect_right(epoch_msec,)
            if i:
                i = i-1
        """
        self.epoch_msec = epoch_msec
        if loose is not None:
            self.loose = loose

    def statistics(self, verbose: int = 0) -> str:
        """Report the count of window changes by attribute,
        attribute by count of changes, and most common values per attribute
        """
        s = self.__str__()
        versions = Dict(1)				# {len(windows): count, ...}
        attrs = Dict(1)					# {attribute: count of value changes}
        for windows in self.d.values():
            versions[len(windows)] += 1  # histogram of record versions
            old_rec = windows[0][3]
            for new in windows[1:]: 	# for each state and next state
                next_rec = new[3]
                for attr in next_rec: 	# for each attribute in next state
                    try:
                        if old_rec[attr] != next_rec[attr]:
                            attrs[attr] += 1  # count differences by attribute
                    except KeyError: 	# attr in next_rec but not in old_rec
                        pass			# ignore as if no change
                old_rec = next_rec
        as_list = list(versions.items())  # [(window_len, count), ...]
        as_list.sort()
        s += f"\nwindow_len: {', '.join([f'({cnt}){ln}' for ln, cnt in as_list])}"
        if not verbose:
            return s
        as_list = list(attrs.items()) 	# [(attribute, change_count), ...]
        as_list.sort(key=lambda x: -x[1])
        s += f"\nversions: {', '.join([f'({cnt}){attr}' for attr, cnt in as_list])}"

        values = Dict(2, 0)  			# {attr: {value: count}}
        for windows in self.d.values():  # for each key
            for window in windows:		# for each time window for that key
                rec = window[3]			# record
                for attr, value in rec.items():  # for each
                    values[attr][value] += 1
        for attr, item in values.items():
            cnt_val = [(cnt, key) for key, cnt in item.items()]
            # most common values first. Don't look at vlue, which might be None
            cnt_val.sort(reverse=True, key=lambda x: x[0])
            lst = []
            for i in range(len(cnt_val)):
                if i > 19:				# limit report to the 20 most frequent values
                    lst.append('...') 	# add an ellipsis to indicate that there were more
                    break
                lst.append(f"({cnt_val[i][0]}){cnt_val[i][1]}")
            s += f"\n{attr}: {', '.join(lst)}."
        return s

    def update(self, poll: callable, backup_poll_msec: int = None) -> bool:
        """Update each item's time-window from a single poll_time of a record for each item.
        Updates must occur in ascending order by polledTime

        Parameters:
            poll				iterable returns the records for a single poll
            backup_poll_msec	poll_msec to use if record['polledTime'] is missing

        Returns:		True if polledTime is seconds; false if milliseconds; None if unknown
        """
        this_poll_msec = None
        time_scale = 1000.0				# msec = time_scale*polledTime
        future_msec = 1000.0*(time()+100*24*60*60)
        rec_cnt = 0						# count of records
        for rec_in in poll:
            rec_cnt += 1
            rec = rec_in.copy()
            if this_poll_msec is None: 	# this_poll_msec not yet known?
                try:					# Yes. get epoch milli-second of the poll
                    this_poll_msec = int(float(rec['polledTime'])*time_scale)
                    if this_poll_msec > future_msec:  # seems far in the future?
                        time_scale = 1.0  # Yes. polledTime is already in msec
                        this_poll_msec = int(float(rec['polledTime'])*time_scale)
                except (KeyError, ValueError):
                    this_poll_msec = backup_poll_msec
                if self.poll_msec is not None and this_poll_msec <= self.poll_msec:  # Updating newer poll(s)?
                    print(f"polledTime={strfTime(this_poll_msec)} is <= " +
                        f"existing polledTime={strfTime(self.poll_msec)}. Ignored.")
                    return None			# ignore the entire update
            # this_poll_msec is acceptable and will be used for all records in the update
            else:						# this_poll_msec is known
                try:
                    pt = int(float(rec['polledTime'])*time_scale)
                except (KeyError, ValueError):
                    pt = this_poll_msec
                if pt != this_poll_msec:
                    raise ValueError(f"record.polledTime={rec['polledTime']} != 1st_record.polledTime={this_poll_msec}")
            if 'polledTime' in rec:
                del rec['polledTime'] 	# do not include polledTime in stored data
            change_msec = avg(self.poll_msec, this_poll_msec)  # change happened between previous and this poll

            try:
                key = self.key_func(rec)
            except KeyError:
                print(f"key missing in {rec}")
                continue
            try:
                windows = self.d[key]
            except TypeError:			# un-hashable type
                raise TypeError(f"primary key={key} is not a hashable type")
            except KeyError: 			# Insert a new item with one open window
                new_window = [change_msec, TimeMachine.infinity, this_poll_msec, rec]
                self.d[key] = [new_window]
                self.add_window(new_window)  # index in other indices
                continue
            else:						# no exception -> Updating an existing item
                window = windows[-1] 	# latest window
                if window[1] == TimeMachine.infinity:  # Open-ended window?
                    if window[3] == rec:  # Yes. With same data?
                        window[2] = this_poll_msec  # Yes, extend last known poll time
                    else:				# No. Close window and add new open window
                        window[1] = change_msec
                        new_window = [change_msec, TimeMachine.infinity, this_poll_msec, rec]
                        windows.append(new_window)
                        self.add_window(new_window)
                else:					# No. Closed window. Add new open window
                    new_window = [change_msec, TimeMachine.infinity, this_poll_msec, rec]
                    windows.append(new_window)
                    self.add_window(new_window)
        if rec_cnt == 0:				# empty update?
            if backup_poll_msec is None:
                print(f"Empty update with no backup_poll_msec was ignored")
                return		# assume total empty update is missing data, ignore the update
            else:
                this_poll_msec = backup_poll_msec

        # Invalidate each time window that is no longer open-ended
        for member in self.d.values():
            last = member[-1]		# most recent time window for this member
            if last[1] == TimeMachine.infinity and last[2] != this_poll_msec:
                # This window was open-ended, but not present in this poll
                last[1] = change_msec  # close the time window
        # table has been successfully updated
        self.poll_msec = this_poll_msec
        self.msecs.append(this_poll_msec)
        return time_scale == 1.0

    def add_window(self, window: list):
        for index in self.indices[1:]:  # for each additional index
            index._insert(window)
