import fs from 'fs'; 
import csvParser from 'csv-parser'; 
import  { createObjectCsvWriter  } from 'csv-writer';

// https://medium.com/@saravanan16498/pivot-and-group-data-sk-yazhini9798-4a63e5cc3dd3
// data is the array of bjects
// groupby is the pivot grouper
// key is the attribute whose values gets transformed to individual keys
// value is the corresponding value of the object attribute will be contained in the tramsformed keys
function pivotBy(data, groupByKeys, pivotKey, pivotValue) {
    let uniqueGroupByValues = [];
	let uniquePivotKeyValues = [];
	let pivotedData = [];

    // Calculate the concatenated string of keys and values 
    function keyValueString(obj) {
        let calcString = '';
        Object.entries(obj).forEach(([key,value]) => {
                if(groupByKeys.includes(key)) {
                    calcString = calcString + `${key}${value}`;
                };
        });
        return calcString;
    };

    // User reduce to create an array of unique values of the data array indexed by groupby (one)
    // Pass through all groupBy attribute values (rows) to get unique values that will become keys (columns) 
    //
    // Use reduce to go through all the values in the object array
    //     reduce((accumulator,currentValue,index, array) => {
    //                 ... accumulator callback function 
    //                 },initialValue);
    //
    // Both data and groupby are defined outside of this helper function
    // In this function definition, uniqueValues is the accumulator and obj is the currentValue. 
    // index and array are not used, so no need to reference in the function 
 
    // Get list of unique key-value strings of the groupByKeys
	uniqueGroupByValues = data.reduce((uniqueValues, obj) => {  // pivotkey = class
        // Under key class, what ar ethe unique values?
		if (!uniqueValues.includes(keyValueString(obj))) { // check if the current object is not yet in the array of unique values
			uniqueValues.push(keyValueString(obj)); // accumulator pushes new unique values to the array
		};
		return uniqueValues; 
	}, []);

    // console.log(uniqueGroupByValues);

    // Like grouper keys, we also need to get all the unique pivot key values
	uniquePivotKeyValues = data.reduce((uniqueValues, obj) => {  // pivotkey = class
        // Under key class, what ar ethe unique values?
		if (!uniqueValues.includes(obj[pivotKey])) { // check if the current object is not yet in the array of unique values
			uniqueValues.push(obj[pivotKey]); // accumulator pushes new unique values to the array
		}
		return uniqueValues; 
	}, []);
   
    //console.log(uniquePivotKeyValues);

    // Item is the current object in the array being iterated over
    uniqueGroupByValues.forEach((item) => {
        pivotedData.push(
            data.reduce((pivotedObj, obj) => {
                if(item === keyValueString(obj)){ // pivotKey = class
                    // reconstruct object with original groupByKeys
                    groupByKeys.forEach((key) => {
                        pivotedObj[key] = obj[key];
                    });
                    // Transposition from rows to colums 
                    pivotedObj[obj[pivotKey]] = obj[pivotValue];
                };
                // Before we return the object, add the missing keys
                uniquePivotKeyValues.forEach((pivotItem) => {
                    if ( !pivotedObj.hasOwnProperty(pivotItem)) {
                        pivotedObj[pivotItem] = '';
                    }
                })
                //console.log(item);
                //console.log(pivotedObj);
                return pivotedObj
            }, {})
        );
    });
    // keys are not sorted, but csv-writer will take care of outputting the values in the proper column!!!
	return pivotedData;
};

/*
//Test data
let data = [
	{ district: 'A', schoolName: 'abc', class: 8, category: 'Male', count: 50 },
	{ district: 'A', schoolName: 'abc', class: 8, category: 'Female', count: 43 },
	{ district: 'A', schoolName: 'abc', class: 9, category: 'Male', count: 38 },
	{ district: 'A', schoolName: 'abc', class: 9, category: 'Female', count: 36 },
	{ district: 'A', schoolName: 'def', class: 10, category: 'Male', count: 56 },
	{ district: 'A', schoolName: 'def', class: 10, category: 'Nonbinary', count: 64 },
	{ district: 'A', schoolName: 'def', class: 10, category: 'Female', count: 48 },
	{ district: 'A', schoolName: 'dif', class: 10, category: 'Female', count: 22 }
];

let pivotedData = [];

console.log('Original object list:');
console.log(data);
console.log('Objects: ');
pivotedData =  pivotBy(data,['district','schoolName','class'], 'category', 'count'); 
console.log('Filtered Object');
console.log(pivotedData);

// Extract keys from the first data object
const header = Object.keys(pivotedData[0]).map(key => ({ id: key, title: key }));

const csvWriter = createObjectCsvWriter({
    path: 'conversion_data_pivoted.csv',
    header,
});

csvWriter.writeRecords(pivotedData)
.then(() => console.log('CSV file written successfully'))
.catch((error) => console.error('Error writing CSV file:', error));
*/

let results = [];
let pivotedData = [];

fs.createReadStream('conversion_data.csv')
  .pipe(csvParser())
  .on('data', (data) => {
    results.push(data);
  })
  .on('end', () => {
    console.log('CSV file read done.');
    //console.log(results);

    pivotedData = pivotBy(results, [ 'MRN','CSN','RESULT_DATETIME' ],'PIVOT_COLUMN','EPIC_DATA_VAL');
    // console.log(pivotedData);

    // Extract keys from the first data object (contains all t he keys, too!)
    const header = Object.keys(pivotedData[0]).map(key => ({ id: key, title: key }));

    const csvWriter = createObjectCsvWriter({
            path: 'conversion_data_pivoted.csv',
            header,
    });

    csvWriter.writeRecords(pivotedData)
        .then(() => console.log('CSV file written successfully'))
        .catch((error) => console.error('Error writing CSV file:', error));

  });