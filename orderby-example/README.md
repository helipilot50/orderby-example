# ORDERBY and GROUPBY example

##Problem
You want to query a set of data in Aerospike and organise the result set using the familiar **ORDERBY** and **GROUPBY** qualifiers found in SQL. But queries on secondary indexes do not directly provide a an orderby or grooup by capability.

##Solution
Use an **Aggregation** query and process the query stream to order and group the results. This works great for *small* result sets as the whole result set will be returned to the client heapspace.

The source code for this solution is available on GitHub, and the README.md 
http://github.com/some place. 

###Build instructions

After cloning the repository, use maven to build the jar files. From the root directory of the project, issue the following command:

mvn clean package -DskipTests
A JAR file will be produced in the directory 'target', this is:

orderby-example-<version>-full.jar
This is a runnable jar complete with all the dependencies packaged.

### Run orderby-example

The JAR file is a runnable JAR.

java -jar orderby-example-1.0-full.jar -h p3 -p 3000 
Options

These are the options:

- -h,--host <arg>  Server hostname (default: localhost)
- -p,--port <arg>  Server port (default: 3000)
- -l,--load		 Load Data.
- -q,--query		 Query Data.
- -o,--order		 Query with Orderby Data.
- -u,--usage       Print usage.


##Discussion

This code example is written in Java and has 3 major functions:

1. Generate data -l, Generate 10,000 records, each with a "shoe colour" and a "shoe size", the shoe colour and shoe size are randomly generated. 
2. Query on a secondary index -q, Queries the data without any ordering and grouping
3. Aggregation Query -o, Queries the data and passes the output stream through a Map function, an Aggregate function and a Reduce function to *Order* and *Group* the result set.

### How it works
The Java application invokes the Aerospike API to perform an aggregation query, using a registered UDF module. 

The UDF module is written in LUA and registered prior to invocation. In this example we register it every time this example is run, but it only needs to be registered once.

Here is code that registers the UDF module. Note that this code looks for the UDF file in a specific location.
```java
/*
 * register UDF
 */
RegisterTask rt = as.client.register(null, "udf/qualifiers.lua", 
				"qualifiers.lua", Language.LUA);
rt.waitTillComplete();

```

The data is queried by creating a `Statement` object and populating it with the `Namespace`, `Set`, and `Bins` to be returned, and the filter to be used with the query. In this case we are using a range filter to filter on show size.

```java
/*
 * Prepare a statement
 */
Statement stmt = new Statement();
stmt.setNamespace(this.namespace);
stmt.setSetName(this.set);
stmt.setBinNames(SHOE_SIZE_BIN, SHOE_COLOUR_BIN);
stmt.setFilters(Filter.range(SHOE_SIZE_BIN, 8, 16));
/*
 * exxecute query
 */
ResultSet rs = this.client.queryAggregate(null, stmt, "qualifiers", "orderby");
/*
 * process results as they arrive from the cluster
 */
while (rs.next()){
	Map<String, List<Map<String,Object>>> result = 
			(Map<String,List<Map<String,Object>>>) rs.getObject();
	for (String mapKey : result.keySet()) {
		List<Map<String, Object>> group = result.get(mapKey);
		System.out.println(mapKey + " size:" + group.size());
		for (Map<String, Object> element : group){
			System.out.println("\t" + element);
		}
	}
}

```
The heavy lifting is don in the UDF to order and group the results as they come from the query stream.

The stream is processed with a `Map()` function, then an `Aggregate()` function and finally a `Reduce()` function.

Let's look at each one, the string them together to process the stream.

#### Map()

The purpose of a `map()` function is to transform the current element in the stream to a new form. In this example we are transforming a `Record` to a `Map`.

While it looks the almost the same, the `mapper()` function is discarding the meta data associated with the `Record`, and retaining only the information we are interested in.
```lua
  local function mapper(rec)
     local element = map()
    element["shoeSize"] = rec["shoeSize"];
    element["shoeColour"] = rec["shoeColour"]
    return element
  end 
```
The `map()` function is invoked on each node in the cluster for *every* element in the stream.

#### Aggregate()
The purpose of the `aggregate()` function is to accumulate a result from the elements in the stream. In this example, the `accumulate()` builds a `Map` where shoe colour use as the key. The value of each entry is a `List` of elements that have that colour. 
```lua
  local function accumulate(currentList, nextElement)
    local shoeColour = nextElement["shoeColour"]
    info("current:"..tostring(currentList).." next:"..tostring(nextElement))
      if currentList[shoeColour] == nil then
        currentList[shoeColour] = list()
      end 
      list.append(currentList[shoeColour], nextElement)
      return currentList
  end
```
The `aggregate()` function is invoked for every element in the stream on each node in the cluster. **Note:** The `currentList` variable is held in RAM, so watch for hign memory usage for large result sets.

#### Reduce()
The `reduce()` function combines all of the results from the stream in to one complete result. It will be invoked on each node in the cluster and a final reduce on the client.

The function `reducer()` simply combines two elements -- in this case two `Maps` that are the output of two `Aggregation()` functions. The code uses the Aerospike utility function `map.merge()`. This function takes the 2 values to merge as parameters, and a 3rd parameter which is a function pointer to merge the elements of the `Map`.  This parameter points to the function `mymerge()` to merge the two elements that are `List`.

```lua
  local function mymerge(a, b)
    return list.merge(a, b)
  end
  
  local function reducer(this, that)
    return map.merge(this, that, mymerge)
  end
```

#### The stream function: orderby()

The stream function `orderby()` is the UDF called by the client. It takes a stream object as a parameter and configures a `map()` function, an `aggregate()` function and a `reduce()` function.

The functions that we have written to implement these stereotypes are passed in as function pointers.

*NOTE:* The `aggregate()` function also takes an additional parameter `map{}`. This in an initial empty `Map` to be populated by the `aggregate()` function.

```lua
function orderby(touples)

	. . .
	  
  return touples:map(mapper):aggregate(map{}, accumulate):reduce(reducer)
end
```