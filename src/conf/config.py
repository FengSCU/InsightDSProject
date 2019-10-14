import yaml


class Config(object):
    def __init__(self):
        with open("conf.yaml", 'r') as stream:
            try:
                self._config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def get_city_list(self):
        return self._config['cities'].keys()

    def get_census_file(self):
        if 'census' in self._config.keys():
            census = self._config['census']
            if 'rawdata' in census.keys():
                return census['rawdata']
        return ""

    def get_census_data_table_name(self):
        if 'census' in self._config.keys():
            census = self._config['census']
            if 'table_name' in census.keys():
                return census['table_name']
        return ""

    def get_crime_data_file(self, city, year):
        city_config = self._config['cities'][city]
        if year in city_config['years']:
            ver = city_config['years'][year]
        else:
            ver = city_config.latest_version

        crime_src_files = city_config['crime-rawdata'][ver]
        return crime_src_files

    def get_city_name(self, city):
        if city in self._config and 'cityname' in self._config[city]:
            return self._config[city]['cityname']
        return city

    def get_mortgage_data_file(self, year):
        mortgage_config = self._config['mortgage']
        rawdata = mortgage_config['rawdata']
        return rawdata + str(year) + "/*"

    def get_county_name_for_city(self, city):
        city_config = self._config['cities'][city]
        return city_config['county_name']

    def get_county_code_for_city(self, city):
        city_config = self._config['cities'][city]
        return city_config['county_code']

    def get_shcool_data_file(self, city):
        city_config = self._config['cities'][city]
        school_src_files = city_config['school-rawdata']
        return school_src_files

    def get_mortgage_tract_table_name(self):
        return self._config['mortgage']['tract_table_name']

    def get_mortgage_data_table_name_for_city(self, city):
        return self._config['cities'][city]['mortgage_data_table_name']

    def get_mortgage_rank_table_name_for_city(self, city):
        return self._config['cities'][city]['mortgage_rank_table_name']
