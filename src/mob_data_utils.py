"""
 Utility functions for data processing. The functions are both
 based on pandas (which means general Python) or Apache Spark.
 The functions which are meant to run in vanilla Python are labelled
 with suffix va while functions running on top of Spark are suffixed
 sp. Utility functions which can agnostic to this issue will bear no such label
"""
import os
from functools import wraps
import random
from collections import namedtuple
from collections import Counter
import math
from datetime import timedelta
import time
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from math import radians, cos, sin, atan2, sqrt

# silence pandas chained assignment warnings
pd.options.mode.chained_assignment = None

# some constants here
DATA_START_DATE = None  # start date of available dataset
DATA_END_DATE = None  # end date of available dataset

# For handling xy coordinates
POINT = namedtuple('POINT', 'point_id, x, y, freq')

# TODO
# 1. add option to generate trips using cell location without clustering
# 2. add parameter to select trips on special days
# 3. add public holidays
# 4. route trips between cell towers
# 5. generate adjustements to adm4 population
# 6. classify cell as residential or commercial/work area


class User:
    """
    contains attributes and functions which will be applied at user level
    """

    def __init__(self, userid=1024, raw_user_data: pd.DataFrame=None, reference_days=None):
        """

        :param userid: an attribute (e.g., imei) will be used as userid
        :param raw_user_data:
        :param reference_days:
        """
        self.userid = userid
        self.raw_events = raw_user_data  # raw user data
        self.trip_days_data = None
        self.events_count_by_day_sum = None
        self.events_count_by_day_detail = None
        self.trips = None
        self.usage_days = None
        self.num_of_days_with_trips = None
        self.reference_days = reference_days
        # For example, minimum distance to consider that a person has moved
        self.misc_params = None
        # Number of days a user travelled
        # Days where user visits more than one location
        self.potential_trip_days = None
        # For each day, take the farthest distance travelled
        self.farthest_distances = None
        self.average_farthest_distance = None
        self.median_farthest_distance = None
        # hourly call counts by base station
        # useful for deciding home/work location
        self.bs_interactions = None
        # For each location, indicates how many calls were made during
        # each category (home, work, transit)
        self.bs_home_work_counts = None
        # home and work cell towers
        self.home_bs = None
        self.home = None
        self.work = None
        self.work_bs = None
        self.total_unique_locations = None
        self.average_unique_locs_perday = None
        self.median_unique_locs_perday = None
        self.trp_clusters_by_day = None
        self.home_work_clusters = None
        self.trips_by_day = None
        self.avg_trps_day = None
        self.avg_trp_dist = None
        self.all_trps = None

    def __hash__(self):
        return hash(self.userid)

    def __eq__(self, other):
        if isinstance(other, User):
            return self.userid == other.userid
        return NotImplemented

    def va_identify_home_work_by_simple_proportion_approach(self):
        """
        In this case, home is Base Station(BS). The method is based on user
        total number of calls made. The calls are categorised as either
        work, transit or home. For this method, the BS with largest proportion
        of home calls is labelled as home.
        :return:
        """
        # retrieve home-work counts
        hw_counts = self.bs_home_work_counts

        # assuming all events are valid, set total_events to total number of user calls
        total_events = self.raw_events.shape[0]
        hw_counts['home_prop'] = hw_counts.apply(lambda x: x['home']/total_events, axis=1)
        hw_counts['work_prop'] = hw_counts.apply(lambda x: x['work']/total_events, axis=1)
        hw_counts.set_index('loc', inplace=True)
        # TODO: currently, this approach doesnt consider within BS frequencies, this has to be fixed to enable
        # TODO: more accurate identification of home/work
        home = hw_counts['home_prop'].idxmax()
        work = hw_counts['work_prop'].idxmax()
        
        return home, work

    def va_set_home_and_work_hours_events(self):
        """
        For each location, count how many events were made during home hours
        and work hours
        A couple of assumptions here
        - Person works regular hours
        - Their home and work location are different
        :return:
        """
        work_hrs = self.misc_params['work_hrs']
        home_hrs = self.misc_params['home_hrs']
        transit_hrs = self.misc_params['transit_hrs']
        loc_hrs = self.bs_interactions

        data = []
        for k, v in loc_hrs.items():
            home = 0
            work = 0
            transit = 0

            for h, g in v.items():
                # count home hours
                if h in work_hrs:
                    work += g
                if h in home_hrs:
                    home += g
                if h in transit_hrs:
                    transit += g
            data.append({'loc': k, 'home': home, 'work': work, 'transit':transit})

        df = pd.DataFrame(data)
        self.bs_home_work_counts = df

    def va_base_station_interactions(self):
        """
        Returns a dict object where the key is location and values
        are hours of the day with counts of calls
        :return:
        """
        datecol = self.misc_params['datetime_col']
        df = self.raw_events
        df['hr'] = df.apply(lambda x: x[datecol].hour, axis=1)
        df['latlon'] = df.apply(lambda x: str(x['Latitude']) + " , " + str(x['Longitude']), axis=1)
        locs = list(df['latlon'].unique())
        locs_hrs = {k: 0 for k in locs}

        for l in locs:
            dfl = df[df['latlon'] == l]
            hrs = list(dfl['hr'].values)
            locs_hrs[l] = Counter(hrs)

        self.bs_interactions = locs_hrs

    def va_set_base_user_attributes(self):
        """

        :return:
        """
        try:
            self.generate_trips_by_day()
            # self.va_base_station_interactions()
            # self.va_set_home_and_work_hours_events()
            self.va_set_farthest_distance()
            # self.va_set_home_work_bs()
        except Exception as e:
            print(e)

    def va_set_home_work_bs(self, how='simple-prop'):
        """
        Set home work BS
        :return:
        """
        if how == 'simple-prop':
            res = self.va_identify_home_work_by_simple_proportion_approach()
            self.home_bs = res[0]
            self.work_bs = res[1]

    def va_assign_usage_category(self):
        """
        The purpose is to separate regular users from business use cases
        :return:
        """

    def va_determine_if_user_is_consistent_overral(self, events_per_day_threshold=5):
        """
        Overral consistency depends on overral number of events per day.
        other measures could be done based on specific cdr types.
        A consistent user has at least 5 events per day.
        :param events_per_day_threshold : Based on average usage and also ability to generate trips
        :return: boolean
        """
        event_dict = self.events_count_by_day_sum

        for k, v in event_dict:
            if v == 0:
                return False

        return True

    def va_generate_events_count_by_day_sum(self, date_col='date'):
        """
        For each day we have the data (reference days as opposed to days user has the data)
        0 indicate no events on that particular day
        :return:
        """
        df_data = self.raw_events
        grp = df_data.groupby([date_col])['cdr_type'].count()
        grp_dict = grp.reset_index().to_dict(orient='list')
        user_event_dates = dict(zip(grp_dict[date_col], grp_dict['cdr_type']))
        all_dates_event_dict = dict([i, 0] for i in self.reference_days)

        for k, v in all_dates_event_dict.items():
            all_dates_event_dict[k] = user_event_dates.get(k, 0)

        self.events_count_by_day_sum = all_dates_event_dict

    def va_filter_out_non_trip_days(self):
        """
        Remove all days from this user's dataset which had no trips.
        No trip days are identified based on distance travelled.
        For instance, if there is only one location visited on that
        day, then there are no trips.
        :return: a df with only trip days data
        """
        df = self.raw_events
        datetime_col = self.misc_params['datetime_col']
        df['date'] = df.apply(lambda x: x[datetime_col].date(), axis=1)
        dates = list(df.date.unique())
        good_dates = []
        for d in dates:
            dfd = df[df.date == d]
            traveled = self.va_check_if_travelled(single_day_df=dfd, dist_threshold=self.misc_params['distance_threshold'],
                                                   min_uniq_locs=self.misc_params['min_unique_locs'])
            if traveled:
                good_dates.append(d)

        if len(good_dates) > 0:
            trp_days_df = df[df.date.isin(good_dates)]
            self.trip_days_data = trp_days_df
            self.potential_trip_days = len(dates)
        else:
            self.trip_days_data = "No trips"
            self.potential_trip_days = 0

    @staticmethod
    def va_distances_travelled(unique_locs=None):
        """
        Daily distances travelled
        :return: a distance matrix containing distances travelled
        """
        df_distances = va_distance_matrix(xy_list=unique_locs)
        distance_cols = [c for c in df_distances.columns if "to" in c]
        df_distances_only = df_distances[distance_cols]
        dist_matrix = df_distances_only.values
        dist_matrix2 = dist_matrix[np.triu_indices(dist_matrix.shape[0], k=1)]

        return dist_matrix2

    def va_check_if_travelled(self, single_day_df=None, dist_threshold=None,
                                       min_uniq_locs=None):
        """
        For a given single day, determine if the person
        travelled far beyond min radius
        :param df_distances:
        :return:
        """
        # check the number of unique locations visited
        # if less than min_unique_locs, this day is discarded
        uniq_locs = va_generate_unique_locs(df=single_day_df)
        if len(uniq_locs) < min_uniq_locs:
            return False

        # if more than min_unique_locs visited, check the distances between the locations
        dist_matrix = self.va_distances_travelled(unique_locs=uniq_locs)

        if np.max(dist_matrix) >= dist_threshold:
            return True

        return False

    @staticmethod
    def detect_trips(clusters=None):
        """
        Detect trips from location history
        :param clusters:
        :return:
        """
        # drop all stops
        [clusters.remove(c) for c in clusters if c.stop_or_stay == 'stop']

        # special cases
        if len(clusters) == 1:
            return None

        # generate trips
        clusters.sort(key=lambda x: x.clust_id, reverse=False)
        
        # split into pairs
        od_pairs = chunks(lst=clusters, n=2)

        # create trips and characterize them
        trips = []
        for i, od in enumerate(od_pairs):
            origin = od[0]
            dest = od[1]
            # just double check that start time is
            # assert origin.last_visit < dest.last_visit
            trp = Trip(id=i, origin=origin, destination=dest, start_time=origin.last_visit, end_time=dest.first_visit)
            trp.set_trip_distance()
            trp.set_trp_time_attributes()
            trips.append(trp)

        return trips
        
    def generate_trips_by_day(self):
        """
        Generate trips for all given days. Also sets other user attributes
        :return:
        """
        datecol = self.misc_params['datetime_col']
        df = self.raw_events
        total_uniq_xy = va_generate_unique_locs(df=df, x=self.misc_params['x'], y=self.misc_params['y'])
        df['date'] = df.apply(lambda x: x[datecol].date(), axis=1)
        dates = sorted(list(self.raw_events.date.unique()))
        num_of_days_with_trips = 0   # record number of days with trips
        dates_trps = {d: None for d in dates}  # record trips finished on that day
        unique_locs_by_day = {d: None for d in dates}  # record unique locations visited each day
        dates_dist = {k: 0 for k in list(df['date'].unique())}  # distances travelled each day
        dates_clusters = {d: None for d in dates}

        for d, t in dates_trps.items():
            # data for this day
            dfd = df[df['date'] == d]

            # get number of unique locations for this day
            uniq_xy = va_generate_unique_locs(df=dfd, x=self.misc_params['x'], y=self.misc_params['y'])
            unique_locs_by_day[d] = len(uniq_xy)

            # generate clusters with stay time
            clusters = cluster_cells_within_radius_trps_version(loc_history=dfd, time_col=datecol,
                                                   x=self.misc_params['x'], y=self.misc_params['y'])

            clusters_with_stay_time = generate_cluster_stay_time(clusters=clusters)

            # generate trips
            trps = self.detect_trips(clusters=clusters_with_stay_time)

            if not trps:
                continue

            # add to list of trips
            dates_trps[d] = trps

            # increment number of trip days
            num_of_days_with_trips += 1

            # add to list of clusters
            dates_clusters[d] = clusters_with_stay_time

            # distances traveled this day
            dist_mtx = self.va_distances_travelled(unique_locs=uniq_xy)
            dates_dist[d] = np.max(dist_mtx)

        # set attributes
        self.usage_days = len(dates)
        self.num_of_days_with_trips = num_of_days_with_trips
        self.trips_by_day = dates_trps
        self.trp_clusters_by_day = dates_clusters
        self.average_unique_locs_perday = np.mean(list(unique_locs_by_day.values()))
        self.median_unique_locs_perday = np.median(list(unique_locs_by_day.values()))
        self.total_unique_locations = len(total_uniq_xy)
        self.median_farthest_distance = np.median(list(dates_dist.values()))
        self.average_farthest_distance = np.mean(list(dates_dist.values()))
        
    def compute_avg_trips_per_day(self, start_end='all'):
        """
        Returns average number of trips per day over a given time interval
        :param start_end = A tuple of start and end time
        :return:
        """
        if start_end == 'all':
            # use all data
            trps = self.trips_by_day
            trps_day = []
            for d, t in trps.items():
                if t:
                    trps_day.append(len(t))

            self.avg_trps_day = np.mean(trps_day)

    def compute_avg_trip_distance(self, start_end='all'):
        """
        1. Average distance within a single day
        2. Average over many days
        :return:
        """

        if start_end == 'all':
            # use all data
            trps = self.trips_by_day
            daily_distance_avgs = []

            for d, t in trps.items():
                if t:
                    distances = [i.trp_distance for i in t]
                    daily_distance_avgs.append(np.mean(distances))

            self.avg_trp_dist = np.mean(daily_distance_avgs)

    def aggregate_trips(self):
        """
        Combine all into a single list
        :return:
        """
        all_trps = []
        for d, trps in self.trips_by_day.items():
            if trps:
                all_trps += trps

        self.all_trps = all_trps

    def generate_home_and_work_clusters(self):
        """
        The purpose of these clusters is to find home and work place
        :return:
        """
        home_hrs = self.misc_params['home_hrs']
        wrk_hrs = self.misc_params['work_hrs']
        trst_hrs = self.misc_params['transit_hrs']
        excluded_dys_for_home_wrk = self.misc_params['excluded_days_for_home_work']

        datecol = self.misc_params['datetime_col']
        clusters = cluster_cells_within_radius_home_work_version(loc_history=self.raw_events, time_col=datecol,
                                                                 x=self.misc_params['x'],home_hrs=home_hrs,
                                                                 wrk_hrs=wrk_hrs, trst_hrs=trst_hrs,
                                                                 exclude=excluded_dys_for_home_wrk,
                                                                 y=self.misc_params['y'])
        self.home_work_clusters = clusters

    @staticmethod
    def categorize_as_home_work(x):
        cats = {'home': x['prop_home'], 'work': x['prop_work'], 'trst': x['prop_trst']}
        max_cat = max(cats, key=cats.get)
        max_val = cats[max_cat]

        if max_val >= 0.60:
            return max_cat
        else:
            return 'ND'

    def set_home_work_from_clusters(self):
        """
        Set user home based on all clusters visited by user
        :return:
        """
        clusters = self.home_work_clusters
        df_data = []
        for c in clusters:
            sum_visit_freqs = c.home_work_visit_freq
            d_pt = {'clust_id': c.clust_id, 'clust_x': c.x, 'clust_y': c.y, 'home': sum_visit_freqs['home'],
                    'work': sum_visit_freqs['work'], 'transit': sum_visit_freqs['transit'], 'total': c.visit_freq}
            df_data.append(d_pt)

        df = pd.DataFrame(df_data)
        # add proportions
        df['prop_home'] = df.apply(lambda x: x['home']/x['total'], axis=1)
        df['prop_work'] = df.apply(lambda x: x['work']/x['total'], axis=1)
        df['prop_trst'] = df.apply(lambda x: x['transit']/x['total'], axis=1)

        # categorise
        df['categ'] = df.apply(lambda x: self.categorize_as_home_work(x), axis=1)

        # sort by total
        df_sorted = df.sort_values(by=['total'], ascending=False)

        # now set home and work
        home = False
        work = False
        for i, row in df_sorted.iterrows():
            if home and work:
                break
            cat = row['categ']
            if not home:
                if cat == 'home':
                    self.home = (row['clust_x'], row['clust_y'])
                    home = True
                    continue
            if not work:
                if cat == 'work':
                    self.work = (row['clust_x'], row['clust_y'])
                    work = True
                    continue

        # if no location is categorised as either home or work, set home loc to work and vice versa
        if not home:
            if work:
                self.home = self.work
        if not work:
            if home:
                self.work = self.home


class CellTower:
    """
    A utility class to generate data based on cells
    """

    def __init__(self, pt: POINT= None, params=None, raw_data=None, cellid=None):
        self.point = pt
        self.cellid = cellid
        self.data = raw_data
        self.activity_by_hr = None
        self.activity_by_hr_with_weekday = None
        self.activity_by_hr_with_h = None
        self.home_work_categorisation = None
        self.params = params
        self.home_work = None
        self.admin_attr = None
        
    def va_get_activity_by_the_hr(self, with_h=False):
        if with_h:
            hr_dict = self.activity_by_hr_with_h
        else:
            hr_dict = self.activity_by_hr
        # add lat, lon
        hr_dict['x'] = self.point.x
        hr_dict['y'] = self.point.y
        
        return hr_dict

    @staticmethod
    def va_base_activity_by_the_hr(df=None, with_h=False):
        """
        Gievn an atomic df, generate hr counts
        :return:
        """
        hr_dict_with_h = {str(k) + "h": 0 for k in range(24)}
        hr_dict = {k: 0 for k in range(24)}

        act_dict = dict(Counter(list(df['hr'].values)))
        for k, v in act_dict.items():
            hr_dict_with_h[str(k) + 'h'] = v
            hr_dict[k] = v

        if with_h:
            return hr_dict_with_h

        return hr_dict

    def va_generate_activity_by_weekday_hr(self):
        """
        Number of events by hour
        :return:
        """
        df = self.data
        datecol = self.misc_params['datetime_col']
        df['hr'] = df.apply(lambda x: x[datecol].hour, axis=1)
        df['day'] = df.apply(lambda x: x[datecol].weekday(), axis=1)
        df['wkday_cat'] = df.apply(lambda x: assign_week_day_category(x['day']), axis=1)

        res = {}
        for w in list(df['wkday_cat'].unique()):
            dfw = df[df['wkday_cat'] == w]
            dictw = self.va_base_activity_by_the_hr(df=dfw, with_h=False)
            res[w] = dictw

        self.activity_by_hr_with_weekday = res
        
    def va_generate_activity_by_hr(self):
        """
        Number of events by hour
        :return:
        """
        df = self.data
        datecol = self.params['datetime_col']
        df['hr'] = df.apply(lambda x: x[datecol].hour, axis=1)
        hr_dict_with_h = {str(k) + "h": 0 for k in range(24)}
        hr_dict = {k: 0 for k in range(24)}

        act_dict = dict(Counter(list(df['hr'].values)))
        for k, v in act_dict.items():
            hr_dict_with_h[str(k) + 'h'] = v
            hr_dict[k] = v

        self.activity_by_hr = hr_dict
        self.activity_by_hr_with_h = hr_dict_with_h

    def va_generate_home_work_categorisation(self):
        """
        Given the hourly activities, classify into 3 classes:
        home, work, transit
        :return:
        """
        hr_dict = self.activity_by_hr
        work_hrs = self.misc_params['work_hrs']
        home_hrs = self.misc_params['home_hrs']
        transit_hrs = self.misc_params['transit_hrs']

        home = 0
        work = 0
        transit = 0

        for h, g in hr_dict.items():
            # count home hours
            if h in work_hrs:
                work += g
            if h in home_hrs:
                home += g
            if h in transit_hrs:
                transit += g
        res = {'x': self.point.x, 'y': self.point.y, 'home': home, 'work': work, 'transit': transit}
        self.home_work_categorisation = res

    def set_home_work(self):
        """
        Attempt to set cell towers
        :return:
        """
        self.home_work = 'home'


class Trip:
    """
    Encapasulates a single trip
    """
    def __init__(self, id=None, start_time=None, origin=None, destination=None, end_time=None):
        """
        
        :param id:
        :param start_time:
        :param origin:
        :param destination:
        :param end_time:
        """
        self.trp_start_time = start_time
        self.trp_origin = origin
        self.trp_dest = destination
        self.trp_end_time = end_time
        self.trp_distance = None
        self.trp_id = id
        self.trp_date = None
        self.trp_hr = None

    def set_trip_distance(self):
        """
        
        :return:
        """
        distance = va_distance(origin=(self.trp_origin.y, self.trp_origin.x),
                               destination=(self.trp_dest.y, self.trp_dest.x))
        self.trp_distance = distance

    def set_trp_time_attributes(self):
        """
        Set base time attributes
        :return:
        """
        self.trp_date = self.trp_start_time.date()
        self.trp_hr = self.trp_start_time.date()

        
class Cluster:
    def __init__(self, method='sequential', clust_type=None, clust_id=None, x=None, y=None,
                 last_visit=None, centre_type=None, members=None):
        """
        :param method: Method generating cluster. Default is sequential (Hartigan Leader)
        :param clust_type: they may be several cluster types (e.g., for trips or determining home)
        :param clust_id: unique cluster ID
        :param radius: Radius used when creating this cluster
        :param last_visit: Time stamp of when this cluster was last visited
        :param x: cluster centre x-Longitude
        :param y: cluster centre y-Latitude
        :param centre_type: whether centre is actual value is centroid
        """
        self.method = method
        self.clust_type = clust_type
        self.clust_id = clust_id
        self.x = x
        self.y = y
        self.last_visit = last_visit
        self.first_visit = last_visit
        self.centre_type = centre_type
        self.site_id = None
        self.adm1_id = None
        self.adm1_name = None
        self.adm2_id = None
        self.adm2_name = None
        self.adm3_name = None
        self.adm3_id = None
        self.grid_1km_id = None
        self.members = members
        self.stay_time = None
        self.stop_or_stay = None
        self.visit_freq = 0
        self.hr_visit_freq = {i: 0 for i in range(0, 24)}
        self.home_work_visit_freq = {'home': 0, 'work': 0, 'transit': 0}

    def __hash__(self):
        return hash(self.clust_id)

    def __eq__(self, other):
        if isinstance(other, Cluster):
            return self.clust_id == other.clust_id
        return NotImplemented

    def update_visit_times(self, time_col=None):
        """
        When a new member is added, update date of last visit
        :return:
        """
        members = self.members
        timestamps = [item[time_col] for item in members]
        self.last_visit = sorted(timestamps, reverse=True)[0]
        self.first_visit = sorted(timestamps, reverse=False)[0]

    def update_cluster_center(self):
        """
        Update cluster centre
        :return:
        """
        pts = [POINT(point_id=m['site_id'], x=m['x'], y=m['y'], freq=1) for m in self.members]
        new_clust_centre = find_geographic_centroid(locations_with_freqs=pts, weighted=False)
        self.x = new_clust_centre.x
        self.y = new_clust_centre.y

    def get_members_count(self):
        """
        Returns number of members in cluster
        :return:
        """
        return len(self.members)

    def stop_stay_categorisation(self, stay_threshold=15):
        """
        Since some places will be considered as trip stops rather than destinations or
        origins. Although this may have very little effect in CDR data.
        :param stay_threshold:  Minimum amount of time to qualify as stay
        :return:
        """
        if self.stay_time < stay_threshold:
            self.stop_or_stay = 'stop'
        else:
            self.stop_or_stay = 'OD'

    def update_hr_visit_freq(self, hr=None):
        """
        Updates visit frequencies
        :return:
        """
        hr_freqs = self.hr_visit_freq
        if hr in hr_freqs:
            hr_freqs[hr] += 1
            self.hr_visit_freq = hr_freqs
            return
        else:
            hr_freqs[hr] = 1
            self.hr_visit_freq = hr_freqs

    def update_visit_freq(self):
        """
        Updates total visit frequency. This is more for verification with counts from the hours.
        :return:
        """
        freq = self.visit_freq
        self.visit_freq = freq + 1
        
    def update_home_work_visit_freqs(self, hr=None, day=None, work_hrs=None, transit_hrs=None, exclude_days=None,
                                     home_hrs=None):
        """
        Updates visit frequencies
        :return:
        """
        if day in exclude_days:
            return

        freqs = self.home_work_visit_freq
        home = freqs['home']
        work = freqs['work']
        transit = freqs['transit']

        if hr in work_hrs:
            work += 1
            freqs['work'] = work
        if hr in home_hrs:
            home += 1
            freqs['home'] = home
        if hr in transit_hrs:
            transit += 1
            freqs['transit'] = transit

        
def timefn(fn):
    @wraps(fn)
    def measure_time(*args, **kwargs):
        t1 = time.time()
        result = fn(*args, **kwargs)
        t2 = time.time()
        print("@timefn:" + fn.__name__ + " took " + str(t2 - t1) + " seconds")
        return result
    return measure_time


def assign_week_day_category(day_int=None):
    """
    Categorise weekday as weekday or weekend
    :param day_int:
    :return:
    """
    if day_int >= 5:
        return 'wkend'

    return 'wkday'


def va_generate_unique_locs(df=None, x=None, y=None):
    """
    Returns unique locations visited (e.g., on a single day)
    :param oneday_df:
    :return:
    """
    df_unique_locs = df.drop_duplicates([x, y])
    # in case Lat, Lon are strings, convert to float
    df_unique_locs[y] = df[y].astype('float64')
    df_unique_locs[x] = df[y].astype('float64')
    
    unique_locs = []
    for idx, row in df_unique_locs.iterrows():
        unique_locs.append(POINT(point_id=idx, x=row[x], y=row[x],freq=-1))

    return unique_locs


def grab_test_users(df: pd.DataFrame=None, how_many=None, user_id="calling_imei"):
    """
    For quick testing aand debugging, select only a few users
    :param df:
    :param how_many:
    :return:
    """
    user_list = list(df[user_id].unique())
    selected_users = random.choices(population=user_list, k=how_many)
    df_selected_users = df[df[user_id].isin(selected_users)]

    return df_selected_users


def va_distance_matrix(xy_list=None):
    """
    Return distance matrix from a dictlist of xy coordinates
    :param xy_list:
    :return: a dataframe style of distance matrix
    """
    df = pd.DataFrame([dict(d._asdict()) for d in xy_list])

    for pt in xy_list:
        pt_id = pt.point_id
        colname = 'to_' + str(pt_id)
        df[colname] = df.apply(lambda x: va_distance(origin=(x['x'], x['y']), destination=(pt.x, pt.y)), axis=1)

    return df


def va_distance(origin=None, destination=None):
    """
    Calculate the Haversine distance.

    Parameters
    ----------
    origin : tuple of float
        (lat, long)
    destination : tuple of float
        (lat, long)

    Returns
    -------
    distance_in_km : float

    Examples
    --------
    >>> origin = (48.1372, 11.5756)  # Munich
    >>> destination = (52.5186, 13.4083)  # Berlin
    >>> round(va_distance(origin, destination), 1)
    504.2
    """
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d


def va_generate_list_of_days(start_date=None, end_date=None):
    """
    Generate a list of each date in the whole dataset to be used as reference for evaluating
    user consistency
    :param start_date:
    :param end_date:
    :return:
    """
    dates = [start_date]
    dt = start_date
    while dt < end_date:
        dt += timedelta(days=1)
        dates.append(dt)
        
    return dates


def va_extract_date_range(df=None, date_col=None):
    """
    Get temporal range of dataset.
    :param df:
    :param date_col:
    :return: tuple (firs date stamp, last date stamp)
    """
    desc = df[date_col].describe()

    return desc['first'].date(), desc['last'].date()


def sp_extract_date_range(df=None, date_col='date'):
    """
    Get temporal range of dataset.
    :param df:
    :param date_col:
    :return: tuple (firs date stamp, last date stamp)
    """
    df2_sorted_asc = df.sort(date_col, ascending=True)
    start = df2_sorted_asc.first()[date_col]
    df2_sorted_desc = df.sort(date_col, ascending=False)
    end = df2_sorted_desc.first()[date_col]
    
    return start, end


def va_add_time_variables(df: pd.DataFrame = None, raw_date_str_col=None, date_fmt=None):
    """
    Assumes df has raw data without datetime object and adds the folowing datetime objects:
    - converts datetime string to datetime
    - adds date
    :param df:
    :param raw_date_str_col:
    :return:
    """

    df['datetime'] = pd.to_datetime(df[raw_date_str_col], format=date_fmt)
    df['date'] = df.apply(lambda x: x.datetime.date(), axis=1)

    return df


def add_lat_lon_to_orange_cdrs(df_cdr=None, df_loc2g=None, df_loc3g=None, outcsv=None):
    """
    Given raw CSV files from orange, add lat and lon from location files to create a second version.
    - Remove spaces in column names
    - Combine 2g and 3g locations
    - keep only events with location details
    """

    # Fix columns-remove spaces
    df_loc2g = remove_spaces_in_colnames(df=df_loc2g)
    df_loc3g = remove_spaces_in_colnames(df=df_loc3g)
    df_cdr = remove_spaces_in_colnames(df=df_cdr)

    # combine 2g and 3g location files
    df_loc = df_loc2g.append(df_loc3g)

    # Merge using cell_ID from location DF and last calling cellid from cdrs
    df_cdr_loc = df_cdr.merge(right=df_loc, right_on='Cell_ID', left_on='last_calling_cellid',
                              how='inner', indicator=True)

    # save to file
    df_cdr_loc.to_csv(path_or_buf=outcsv, index=False)

    # some stats
    cell_ids_cdr = list(df_cdr.last_calling_cellid.unique())
    cell_ids_loc = list(df_cdr.Cell_ID.unique())
    cell_ids_matched = list(df_cdr.last_calling_cellid.unique())
    print('=' * 70)
    print('Number of events before in raw CDRs file: {:,} '.format(df_cdr.shape[0]))
    print('Number of unique cell-IDs in raw events ==> {:,}'.format(len(cell_ids_cdr)))
    print('Number of unique cell-IDs in location file ==> {:,}'.format(len(cell_ids_loc)))
    print('Number of cell-IDs which MATCHED ==> {:,}'.format(len(cell_ids_matched)))
    print('=' * 70)

    print('Done and saved to this file: {}'.format(outcsv))


def remove_spaces_in_colnames(df=None):
    """
    Replace spaces with underscore in column names
    :param df:
    :return:
    """
    cols_o = list(df.columns)
    new_cols = {}
    for c in cols_o:
        new_c = c.replace(' ', '_')
        new_cols[c] = new_c

    df.rename(columns=new_cols, inplace=True)

    return df


def combine_csv_files_pandas_way(folder_name=None, save_to_csv=False, out_csv=None):
    """
    Read csv files as dataframes and then combine them and save into a new csv file
    """
    # Get list of csv files
    csv_files = []
    for f in os.listdir(folder_name):
        csv_path = os.path.join(folder_name, f)
        if f.endswith('csv'):
            csv_files.append(csv_path)

    # Combine dataframes
    df_concat = pd.concat([pd.read_csv(f) for f in csv_files])

    if save_to_csv:
        df_concat.to_csv(out_csv, index=False)
        return out_csv
    else:
        return df_concat


def find_geographic_centroid(locations_with_freqs=None, weighted=False):
    """
    Finds geographic centre, wither weighted by location visit frequency or not.
    :param radius: Radius (Km) for  for spanning locations
    :param locations_with_freqs: For finding weighted location
    :return:
    """

    lat = []
    lon = []
    sum_of_weights = 0
    for l in locations_with_freqs:
        if weighted:
            w = l.freq
        else:
            w = 1

        lat.append(l.y*w)
        lon.append(l.x*w)
        sum_of_weights += w

    y = sum(lat) / sum_of_weights
    x = sum(lon) / sum_of_weights
    pt = POINT(point_id='virtual_loc', x=x, y=y, freq=-1)
    return pt


def cluster_cells_within_radius_home_work_version(radius=2, loc_history: pd.DataFrame=None, time_col=None,
                                                  x=None, y=None, wrk_hrs=None, home_hrs=None, trst_hrs=None,
                                                  exclude=None):
    # sort the events by timestamp
    loc_history.sort_values(by=time_col, ascending=True, inplace=True)

    # initiate some cluster variables
    clusters = []

    # initiate clusters by assigning first element to first cluster
    first_loc = loc_history.iloc[0]
    first_clust = Cluster(clust_id=1, x=first_loc[x], y=first_loc[y], last_visit=first_loc[time_col],
                          members=[{'x': first_loc[x], 'y': first_loc[y], 'site_id': first_loc['site_id'],
                                    time_col: first_loc[time_col]}])
    first_clust.update_visit_freq()
    first_clust.update_home_work_visit_freqs(hr=first_loc[time_col].hour, day=first_loc[time_col].weekday_name,
                                             work_hrs=wrk_hrs, transit_hrs=trst_hrs, exclude_days=exclude,
                                            home_hrs=home_hrs)
    clusters.append(first_clust)

    # Loop through loc_history
    for i, row in loc_history[1:].iterrows():
        current_member = {'x': row[x], 'y': row[y], 'site_id': row['site_id'],
                          time_col: row[time_col]}

        # get distances to other clusters
        min_dist = 1000
        nearest_clust = None
        for c in clusters:
            dist = va_distance(origin=(c.y, c.x), destination=(current_member['y'], current_member['x']))
            if dist < min_dist:
                min_dist = dist
                nearest_clust = c

        # add to existing cluster and update
        if min_dist <= radius:
            # add new member
            nearest_clust.members.append(current_member)

            # update cluster centre
            nearest_clust.update_cluster_center()

            # update visit frequency
            nearest_clust.update_visit_freq()

            # update hour visit frequencies
            nearest_clust.update_home_work_visit_freqs(hr=row[time_col].hour, day=row[time_col].weekday_name,
                                                     work_hrs=wrk_hrs, transit_hrs=trst_hrs, exclude_days=exclude,
                                                     home_hrs=home_hrs)
            # update cluster timestamp
            nearest_clust.update_visit_times(time_col=time_col)
            continue

        # create new cluster and add  to cluster
        recent_clust = clusters[-1]
        new_clust_id = recent_clust.clust_id + 1
        new_clust = Cluster(clust_id=new_clust_id, x=row[x], y=row[y], last_visit=row[time_col],
                            members=[current_member])
        # update visit frequency
        new_clust.update_visit_freq()

        # update hour visit frequencies
        new_clust.update_home_work_visit_freqs(hr=row[time_col].hour, day=row[time_col].weekday_name,
                                                  work_hrs=wrk_hrs, transit_hrs=trst_hrs, exclude_days=exclude,
                                                  home_hrs=home_hrs)
        clusters.append(new_clust)

    return clusters


def cluster_cells_within_radius_trps_version(radius=1, loc_history: pd.DataFrame=None, time_col=None, x=None, y=None):
        """
        Given location history in a time interval, cluster them.
        :param radius: Radius (Km) for  for spanning locations
        :param locations_with_freqs: For finding weighted location
        :return:
        """
        # sort the events by timestamp
        loc_history.sort_values(by=time_col, ascending=True, inplace=True)

        # initiate some cluster variables
        clusters = []

        # initiate clusters by assigning first element to first cluster
        first_loc = loc_history.iloc[0]
        first_clust = Cluster(clust_id=1, x=first_loc[x], y=first_loc[y], last_visit=first_loc[time_col],
                                  members=[{'x': first_loc[x], 'y':first_loc[y],'site_id':first_loc['site_id'],
                                            time_col: first_loc[time_col]}])
        first_clust.update_visit_freq()
        first_clust.update_hr_visit_freq(hr=first_loc[time_col].hour)
        clusters.append(first_clust)

        # loop through loc_history
        for i, row in loc_history[1:].iterrows():
            current_member = {'x': row[x], 'y': row[y], 'site_id': row['site_id'],
                              time_col: row[time_col]}

            # get most recent cluster
            recent_clust = clusters[-1]

            # distance between recent clust centre and current loc
            dist = va_distance(origin=(recent_clust.y, recent_clust.x),
                               destination=(current_member['y'], current_member['x']))

            # add to existing cluster and update
            if dist <= radius:
                # add new member
                recent_clust.members.append(current_member)

                # update cluster centre
                recent_clust.update_cluster_center()

                # update visit frequency
                recent_clust.update_visit_freq()

                # update hour visit frequencies
                recent_clust.update_hr_visit_freq(hr=row[time_col].hour)
                
                # update cluster timestamp
                recent_clust.update_visit_times(time_col=time_col)
                continue

            # create new cluster and add  to cluster
            new_clust_id = recent_clust.clust_id + 1
            new_clust = Cluster(clust_id=new_clust_id, x=row[x], y=row[y], last_visit=row[time_col],
                                members=[current_member])
            # update visit frequency
            new_clust.update_visit_freq()

            # update hour visit frequencies
            new_clust.update_hr_visit_freq(hr=row[time_col].hour)

            clusters.append(new_clust)

        return clusters


def generate_cluster_stay_time(clusters=None, stay_threshold=None):
    """
    Estimates how long a user stayed at some cluster. The
    final cluster-last visited place is given a stay time of
    1 day.
    :param clusters: Clusters to evaluate
    :return:
    """
    res = []
    for i, x in enumerate(clusters):
        diff = (x.first_visit - clusters[i - 1].first_visit).total_seconds()
        stay_time = int(diff / 60)
        res.append(stay_time)

    for s, c in zip(res[1:], clusters):
        c.stay_time = s
    clusters[-1].stay_time = 1440

    # categorise as stay or stops
    [c.stop_stay_categorisation() for c in clusters]

    return clusters


def chunks(n=None, lst=None):
    """
    Split list of clusters into trips
    :param n:
    :param lst:
    :return:
    """
    if len(lst) == 2:
        return [lst]

    n = min(n, len(lst) - 1)
    return [lst[i:i + n] for i in range(len(lst) - n + 1)]


def agreggate_user_trips(trp_list=None):
    """
    Returns a three elements: trips with all attributes,
    Origins spatial attributes, Destination spatial attributes
    which will form input into to get admin boundaries
    :param trp_list:
    :return:
    """
    trps_full = []
    trps_origins = []
    trps_dest = []

    for i, trp in enumerate(trp_list, start=1):
        d_pt = {'trp_id': i, 'Ox': trp.trp_origin.x, 'Oy': trp.trp_origin.y, 'Dx': trp.trp_dest.x,
                'Dy': trp.trp_dest.y, 'date': trp.trp_date, 'hr':trp.trp_hr, 'start': trp.trp_start_time,
                'dist': trp.trp_distance}

        trps_full.append(d_pt)
        trps_origins.append({'trp_id': i, 'Ox': trp.trp_origin.x, 'Oy': trp.trp_origin.y})
        trps_dest.append({'trp_id': i, 'Dx': trp.trp_dest.x, 'Dy': trp.trp_dest.y})

    return pd.DataFrame(trps_full), pd.DataFrame(trps_origins), pd.DataFrame(trps_dest)


def create_shapely_point_geometry(x,y):
    return Point(x, y)


def convert_to_geodf_using_geopandas(df=None, x=None, y=None):
    """
    Converts a pandas DF to  a geopandas  point dataframe
    :param df:
    :param x:
    :param y:
    :return:
    """
    geometry = [Point(xy) for xy in zip(df[x], df[y])]
    df = df.drop([x, y], axis=1)
    crs = {'init': 'epsg:4326'}
    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

    return gdf


def add_admin_attributes_to_random_latlon(admin_shp=None, df_with_latlon=None, lon=None, lat=None, required_cols=None):
    """
    Do a spatial join between a geopandas DF with latlon representing trip ODs
    and an administrative level of interest (e.g., level 3 or 4)
    :param admin_shp: the shapefile with srs 4326
    :param df_with_latlon: the geopandas
    :return:
    """
    # read theshapefile
    gdf_admin = gpd.read_file(filename=admin_shp)

    # convert latlon df two geopandas df
    gdf = convert_to_geodf_using_geopandas(df=df_with_latlon, x=lon, y=lat)
    df = gpd.sjoin(gdf, gdf_admin, how="inner", op='within')
    if required_cols:
        df = df[required_cols]
    return df


def aggregate_ODs_to_admin_level(all_user_trips=None, admin_shp_file=None, aggreg_col_id='admin4Pcod',
                                 aggreg_col_name='admin4Name'):
    """
    Aggregates ODs to a given admin level
    :return:three DFs: OD, outflows, inflows, aggregated at admin level
    """
    df_trps, dfO, dfD = agreggate_user_trips(trp_list=all_user_trips)
    required_cols = ['trp_id', 'geometry', 'index_right', 'OBJECTID', 'admin4Name',
                     'admin4Pcod', 'admin4RefN', 'admin3Name', 'admin3Pcod', 'admin2Name',
                     'admin2Pcod', 'admin1Name', 'admin1Pcod']
    dfO_with_att = add_admin_attributes_to_random_latlon(admin_shp=admin_shp_file, df_with_latlon=dfO,
                                                            lon='Ox', lat='Oy', required_cols=required_cols)
    dfD_with_att = add_admin_attributes_to_random_latlon(admin_shp=admin_shp_file, df_with_latlon=dfD,
                                                            lon='Dx', lat='Dy', required_cols=required_cols)
    # join the Origins and Destinations
    dfOD = dfO_with_att.merge(right=dfD_with_att, on='trp_id', suffixes=['_o', '_d'])

    # create OD column for aggregation IDs and Names
    origin_col_id =  aggreg_col_id + '_o'
    dest_col_id =  aggreg_col_id + '_d'
    origin_col_name = aggreg_col_name + '_o'
    dest_col_name = aggreg_col_name + '_d'

    # get outflows
    df_grp_outflows = dfOD.groupby([origin_col_id, origin_col_name])['trp_id'].count()
    df_outflows = df_grp_outflows.reset_index()
    df_outflows.rename(columns={"trp_id": "count"}, inplace=True)

    # inflows
    df_grp_inflows = dfOD.groupby([dest_col_id, dest_col_name])['trp_id'].count()
    df_inflows = df_grp_inflows.reset_index()
    df_inflows.rename(columns={"trp_id": "count"}, inplace=True)

    # Flows
    dfOD['agg_id'] = dfOD.apply(lambda x: x[origin_col_id] + ',' + x[dest_col_id], axis=1)
    df_grp = dfOD.groupby([origin_col_name, dest_col_name])['agg_id'].count()
    df_flows = df_grp.reset_index()
    df_flows.rename(columns={'agg_id': 'count'}, inplace=True)

    return df_flows, df_outflows, df_inflows


def convert_hr_freqs_into_work_home_scores(hr_freqs_dict=None, work_hrs=None, home_hrs=None, transit_hrs=None):
    """
    Takes a dict object with keys as hours and values as counts and aggregates by work, home and transit categories
    :param work_hrs: A list of hours considered to represent time a caller is at work
    :param home_hrs: A list of hours considered to represent time a caller is home
    :param transit_hrs: These are hours the caller is neither home or at work
    :return: A dict object where keys are work_hrs, home_hrs and transit_hrs
    """
    out_dict = {'home': 0, 'work': 0, 'transit': 0}

    for k, v in hr_freqs_dict:
        home = out_dict['home']
        work = out_dict['work']
        transit = out_dict['transit']

        if k in work_hrs:
            work += v
            out_dict['work'] = work
            continue
        if k in home_hrs:
            home += v
            out_dict['home'] = home
            continue
        if k in transit_hrs:
            transit += v
            out_dict['transit'] = transit

    return out_dict


def calculate_distance(pt1, pt2):
    """
    Computes distance between two geographic coordinates
    :param pt1: [Lat,Lon] for first point
    :param pt2: [Lat,Lon] for second
    :returns distance in km between the two points
    """
    # Radius of the earth in km (Hayford-Ellipsoid)
    EARTH_RADIUS = 6378388 / 1000

    d_lat = radians(pt1[0] - pt2[0])
    d_lon = radians(pt1[1] - pt2[1])

    lat1 = radians(pt1[0])
    lat2 = radians(pt2[0])

    a = sin(d_lat / 2) * sin(d_lat / 2) + \
        sin(d_lon / 2) * sin(d_lon / 2) * cos(lat1) * cos(lat2)

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return c * EARTH_RADIUS