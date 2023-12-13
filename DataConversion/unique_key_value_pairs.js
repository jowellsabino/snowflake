// Sample array of objects
const arrayOfObjects = [
  { name: 'John', age: 25 },
  { name: 'Alice', age: 30 },
  { name: 'John', age: 25 }, // Duplicate entry
  { name: 'Bob', age: 22 },
];

// Using reduce to get unique key-value pairs
const uniqueKeyValues = arrayOfObjects.reduce((accumulator, currentObj) => {
  let groupBy = '';
  Object.entries(currentObj).forEach(([key, value]) => {
    groupBy = groupBy + `${key}${value}`;
    const keyString = `${key}:${value}`;
    if (!accumulator.uniqueKeys.has(keyString)) {
      accumulator.result.push({ [key]: value });
      accumulator.uniqueKeys.add(keyString);
    }
  });
  return accumulator;
}, { result: [], uniqueKeys: new Set() }).result; //get the results part of this Object

console.log(uniqueKeyValues);
