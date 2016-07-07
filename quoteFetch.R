#!/usr/bin/Rscript --slave
library('RMongo')
library('tseries')
library('quantmod')

router <- '127.0.0.1'
theCol <- 'crspData'

crspMongo <- mongoDbConnect('priceData',host=router,port="27017")
startDate <- dbGetDistinct(crspMongo, theCol,'date')
startDate <- startDate[order(startDate)]
print(startDate)
startDate <- as.Date(startDate[length(startDate)]) + 1
startDate <- format(startDate, format="%Y-%m-%d")
print(startDate)
if (length(startDate) == 0)
{
  startDate <- "2014-01-01" #arbitrary start date
}
endDate <-  Sys.Date()
endDate <- format(endDate, format="%Y-%m-%d")

theSyms <- stockSymbols(exchange = c("AMEX", "NASDAQ", "NYSE"),sort.by = c("Exchange", "Symbol"), quiet = FALSE)

for(i in 1:nrow(theSyms))
{
	test <- tryCatch(temp <- get.hist.quote(instrument=theSyms$Symbol[i], start=startDate, end=endDate,provider="yahoo", quote = c("Open", "High", "Low", "Close", "Volume"),drop=TRUE), error = function(e) print("ERROR"))
	if(length(test) != 0)
	{
			if (test != "ERROR")
			{
				theIndex <- matrix(as.character(index(temp)))
				theOpen <- round(matrix(temp$Open),4)
				theClose <- round(matrix(temp$Close),4)
				theHigh <- round(matrix(temp$High),4)
				theLow <- round(matrix(temp$Low),4)
				theVol <- round(matrix(temp$Volume),4)
				temp <- cbind(theIndex,theSyms$Symbol[i],theOpen,theClose,theHigh,theLow,theVol)
				temp <- na.omit(temp)
				for(j in 1:nrow(temp))
				{ 
		 			tempInsert <- paste("{'date': '",temp[j,1],"','ticker': '",paste("$",temp[j,2],sep=""),"','OPENPRC': '",temp[j,3],"','PRC': '",temp[j,4],"','HIGH': '",temp[j,5],"','LOW': '",temp[j,6],"','VOLUME': '",temp[j,7],"'}",sep="")
					dbInsertDocument(crspMongo, theCol, tempInsert)
				}
			}
	}
}
dbDisconnect(crspMongo)
