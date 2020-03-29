# MyProjects


## STEPS TO RUN UPSTOX_BARCHART
1. git clone https://github.com/nikhil2403/MyProjects
2. cd into upstox-barchart
3. run command -->   mvn spring-boot:run -Dspring-boot.run.arguments="STOCK1 STOCK2 STOCK3"
## output will be displayed on terminal


#### DESIGN CRITERIA #########
This program will read the json file which is present under root directory.
In order to get the output of specific stocks enter them as arguments to main run command .For ex
mvn spring-boot:run -Dspring-boot.run.arguments="XETHZUSD XXBTZUSD"
This will print the bar logs for two stocks i.e XETHZUSD and XXBTZUSD

The program will load the list of  trades and group them with respect to each stock.
Although it is mentioned that tades would be sorted in time series but just to be sure programmatically,
Sorted TreeSet is used to store the list of trades sorted by TS2 field.
For each stock and its lost of trades ,a new task is generated and sent to an Executor.
The executor will create tasks  minimum of (number of available cores, number of stocks to generate report)
Max limit is set on number of available cores because creating threads more than number of cores is wastage of resources as all the cores would be busy all the time and thread context switching will not bear much advantage.
Logger is not used and instead System.out.print is used to log to console because log.info was logging in slightly less readable format.
Comments have been added in appropriate places in code.
