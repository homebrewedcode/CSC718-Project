#include <omp.h>
#include <iostream>
#include <fstream>
#include <map>
#include <cstdio>
#include <ctime>

using namespace std;

/*Print out our summary data*/
void print_summary(double duration, int category_count, int total_records, int max_count, int min_count, string max_string, string min_string)
{
	cout << endl << "=======================SUMMARY=========================" << endl << endl;
	cout << "Total parallel code (OpenMP) exectuation time:  " << duration << " seconds" << endl;
	cout << "Total number of records parsed: " << total_records << endl;
	cout << "The total number of crime categories: " << category_count << endl;
	cout << "The crime category with the most occurences: "<< endl;
	cout << "\t" << max_string << " with a count of " << max_count << endl;
	cout << "The crime category with the least occruences:" << endl;
	cout << "\t" << min_string << " with a count of " << min_count << endl;
	cout << endl << "=======================================================" << endl << endl;
}

/*Print out each value and track min/max string values and counts*/
void print_values(map<string, int> &data, string &max_string, int &max_count, string &min_string, int &min_count)
{

	bool first_pass = true;

	for(auto& name : data)
	{
		if(name.second > max_count)
		{
			max_string = name.first;
			max_count = name.second;
		}
		if(first_pass)
		{
			min_string = name.first;
			min_count = name.second;
			first_pass = false;
		}
		else if(name.second < min_count)
		{
			min_string = name.first;
			min_count = name.second;
		}

		cout << name.first << " => " << name.second << endl;
	}

}


/*Open the data file and parse, insert data into map data structure*/
void parse_file(map<string, int> &data, int &category_count, int &total_records, string file_names[])
{

    /*each thread will build a local map that will be merged with the main map at the end,
     *reduction added to collect total_record data*/
    #pragma omp parallel for default(shared) reduction(+:total_records)
    for(int i = 0; i < 4; i++)
    {
      if(omp_get_thread_num() == 0)
      {
        cout << omp_get_num_threads() << " threads have been spawned for file parsing" << endl;
      }

      ifstream file;
    	file.open(file_names[i]);
    	string value, search, junk;
      map<string, int> local_data;


    	//parse our file until we reach EOF
    	while( file.good() )
    	{
    		getline(file, junk, ',');
    		getline(file, junk, ',');
    		getline(file, junk, ',');
    		getline(file, junk, ',');
    		getline(file, value, ',');

    		auto search = local_data.find(value);

    		/*First we strip out bad values, CSV has some strenous data, even after cleaning it up a bit.
    		 *Then we add new string value and count of 1 to map, or we increment count of value if found in map*/
    		if(!value.empty() && value != "NULL" && value.length() < 80)
    		{
    			if(search != data.end())
    			{
    				local_data[value] += 1;
    				total_records += 1;
    			}
    			else
    			{
    				local_data.insert({value, 1});
    				total_records += 1;
    			}
    		}
    		getline(file, value);
    	}

    	file.close();

      //only allow 1 thread to access map at a time to prevent race issues.
      #pragma omp critical
      {
        for(auto& name : local_data)
        {
          auto search = data.find(name.first);

          if(search != data.end())
          {
            data[name.first] += name.second;
          }
          else
          {
            data.insert({name.first, name.second});
          }
        }
      }
    }

    category_count = data.size();
}



int main(int argc, char* argv[])
{

	double duration, start_time, stop_time;//for timing
	int max_count = 0;//track the count of the highest crime occurence
  int	min_count = 0;//track the count of the minimum crimce occurence
  int	category_count = 0;//keep track of the count on each category
	int total_records = 0;//track all records
	string max_string;//String value with most occurences
  string min_string;//String value of least occurrences
	map<string, int> data;//data structure to collect values
  string file_names[] {argv[1], argv[2], argv[3], argv[4]};//compile a list of files to parse

  //start the clock
  start_time = omp_get_wtime();

  //parse the file
	parse_file(data, category_count, total_records, file_names);

	//print values and calculate min/max counts and values
	print_values(data, max_string, max_count, min_string, min_count);

  //stop clock
  stop_time = omp_get_wtime();
  duration = stop_time - start_time;

	//print application summary
	print_summary(duration, category_count, total_records, max_count, min_count, max_string, min_string);


}
