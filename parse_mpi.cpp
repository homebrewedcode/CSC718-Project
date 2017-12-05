#include <iostream>
#include <fstream>
#include <map>
#include <cstdio>
#include <ctime>
#include <mpi.h>
using namespace std;

void print_summary(double duration, int category_count, int total_records, int max_count, int min_count, string max_string, string min_string)
{
	cout << endl << "=======================SUMMARY=========================" << endl << endl;
	cout << "Total sequential code exectuation time:  " << duration << " seconds" << endl;
	cout << "Total number of records parsed: " << total_records << endl;
	cout << "The total number of crime categories: " << category_count << endl;
	cout << "The crime category with the most occurences: "<< endl;
	cout << "\t" << max_string << " with a count of " << max_count << endl;
	cout << "The crime category with the least occruences:" << endl;
	cout << "\t" << min_string << " with a count of " << min_count << endl;
	cout << endl << "=======================================================" << endl << endl;
}


/*Print each value to file and track min/max string values and counts*/
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
void parse_file(map<string, int> &data, int &total_records, string file_name, string processor_name)
{

	ifstream file;
	file.open(file_name);
	string value, search, junk;

  cout << "Processor " << processor_name << " is now parsing file " << file_name << endl;

	//parse our file until we reach EOF
	while( file.good() )
	{

		getline(file, junk, ',');
		getline(file, junk, ',');
		getline(file, junk, ',');
		getline(file, junk, ',');
		getline(file, value, ',');

		auto search = data.find(value);

		/*First we strip out bad values, CSV has some strenous data, even after cleaning it up a bit.
		 *Then we add new string value and count of 1 to map, or we increment count of value if found in map*/
		if(!value.empty() && value != "NULL" && value.length() < 80)
		{
			if(search != data.end())
			{
				data[value] += 1;
				total_records += 1;
			}
			else
			{
				data.insert({value, 1});
				total_records += 1;
			}
		}
		getline(file, value);
	}

	cout << "Processor " << processor_name << " has finished parsing file " << file_name << " closing file" << endl;
	file.close();

}


//creates a data file for each process, these will later be compiled into one data structure
void create_data_files(const map<string, int> &data, int rank, string processor_name)
{

	ofstream results;

	cout << "Processor " << processor_name << " is palcing data into results file." << endl;

	switch(rank)
	{
		case 0:
			results.open("results_0.csv", ofstream::out | ofstream::trunc);
			break;
		case 1:
			results.open("results_1.csv", ofstream::out | ofstream::trunc);
			break;
		case 2:
			results.open("results_2.csv", ofstream::out | ofstream::trunc);
			break;
		case 3:
			results.open("results_3.csv", ofstream::out | ofstream::trunc);
			break;

	}

	for(auto& name : data)
	{
		results << name.first << "," << name.second << "," << endl;
	}

	cout << "Processor " << processor_name << " has finishd building data file, closing stream." << endl;

	results.close();

}


//parse the results of all result files into single map data structure an the master process
void parse_results(map<string, int> &data, int &category_count)
{
	string file_array[] = {"results_0.csv", "results_1.csv", "results_2.csv", "results_3.csv"};

	//parse each file
	for(string file_name : file_array)
	{
		ifstream file;
		file.open(file_name);
		string value, string_total;
		int total;

		cout << "Processing " << file_name << endl;

		//parse our file until we reach EOF
		while( file.good() )
		{

			getline(file, value, ',');
			getline(file, string_total, ',');

			total = stoi(string_total);

			auto search = data.find(value);

			/*Ensure there are no bad values, then sum the total for existing records,
			* or add the new record with current total if it doesn't exist yet*/
			if(!value.empty() && value != "NULL" && value.length() < 80)
			{
				if(search != data.end())
				{
					data[value] += total;
				}
				else
				{
					data.insert({value, total});
					category_count++;
				}
			}
			getline(file, value);
		}

		cout << "Processing of " << file_name << " complete, closing filestream" << endl;
		file.close();

	}
}

int main(int argc, char* argv[])
{
		//MPI variables and setup
		int numprocs, rank, namelen;
  	char processor_name[MPI_MAX_PROCESSOR_NAME];
  	MPI_Init(&argc, &argv);
  	MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
  	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  	MPI_Get_processor_name(processor_name, &namelen);

		int max_count = 0;//track the count of the highest crime occurence
    int	min_count = 0;//track the count of the minimum crimce occurence
    int	category_count = 0;//keep track of the count on each category, global version for reduce
		int global_total_records, total_records = 0;//track all records, global version for reduce
		double duration = 0.0;//For timekeeping
		string max_string;//String value with most occurences
    string min_string;//String value of least occurrences
		map<string, int> data;//data structure to collect values


  	printf("\nHello from process %d on %s out of %d.\n", rank, processor_name, numprocs);

	//open the data file based

	//start the clock
	duration = -MPI_Wtime();

	//select the file to parse based on rank and command line parameter
	switch(rank)
	{
		case 0:
			cout << "Opening file " << argv[1] << " on Processor " << processor_name << endl;
			parse_file(data, total_records, argv[1], processor_name);
			break;
		case 1:
			cout << "Opening file " << argv[2] << " on Processor " << processor_name << endl;
			parse_file(data, total_records, argv[2], processor_name);
			break;
		case 2:
			cout << "Opening file " << argv[3] << " on Processor " << processor_name << endl;
			parse_file(data, total_records, argv[3], processor_name);
			break;
		case 3:
			cout << "Opening file " << argv[4] << " on Processor " << processor_name << endl;
			parse_file(data, total_records, argv[4], processor_name);
			break;

	}

	//wait on everyone finishes before we begin crunching data
	MPI_Barrier(MPI_COMM_WORLD);

	if(rank == 0)
	{
		cout << "Compiling category and totoal record global data with MPI_Reduce across all processors." << endl;
	}

	//Collect the total record count from all processors
	MPI_Reduce(
		&total_records,
		&global_total_records,
		1,
		MPI_INT,
		MPI_SUM,
		0,
		MPI_COMM_WORLD
	);

	//build data files from each process
	create_data_files(data, rank, processor_name);

	//wait on all processes to finish building their files before we parse results.
	MPI_Barrier(MPI_COMM_WORLD);

	if(rank == 0)
	{
			//clear the map for rebuilding with entire list of results
			data.clear();

			//build new map from each processes results file
			cout << "Parsing result files" <<  endl;
			parse_results(data, category_count);

			cout << endl << "Printing compiled results" << endl << "=====================================================" << endl << endl;

			//print values and calculate min/max counts and values
			print_values(data, max_string, max_count, min_string, min_count);

			//caputure running time
			duration += MPI_Wtime();

			//print the results
			print_summary(duration, category_count, global_total_records, max_count, min_count, max_string, min_string);
	}

	MPI_Finalize();

}
