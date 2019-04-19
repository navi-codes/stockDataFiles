const fs = require('fs')
const csv = require('fast-csv')
const moment = require('moment')
const _ = require('lodash');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
companyName=null
previousTime=null
fileDataHash = []

function execute(){
  fs.createReadStream('file3.csv')
  .pipe(csv())
  .on('data',function(data){
    fileData(data)
  })
  .on('end',function(data){
    endOfFile(fileDataHash[fileDataHash.length-1])
    writeFile(companyName)
    console.log('copy finished')
  })
}

function fileData(data){
  writeData={
    ticker: data[0],
    date: data[1],
    time: data[2],
    open: data[3],
    high: data[4],
    low: data[5],
    close: data[6],
    volume: data[7],
    openInterest: data[8]
  }
  newData = checkNextTime(writeData)
  pushUpdatedDate(newData)
  fileDataHash.push(writeData)
}

function pushUpdatedDate(newData){
  if (!newData.res){
    nextTime=newData.startTime
    _.times(newData.timeGap, () => {
      nextTime = timeIncrement(nextTime)
      updateData = updateFile(newData,writeData,nextTime)
      fileDataHash.push(updateData)
    })
  }
}

function checkNextTime(checkdata){
  if(checkdata.ticker != companyName){
    endOfFile(checkdata)
  }
  if(previousTime==null){
    previousTime=checkdata.time
    previousData = checkdata.close
    previoueVolume = checkdata.volume
    return{res:true}
  }else{
    checkTime = timeIncrement(previousTime)
  }
  if(checkdata.time==checktime){
    previousTime=checkdata.time
    previousData = checkdata.close
    previoueVolume = checkdata.volume
    return{res:true}
  }else{
    return(timeDifference(checkdata))
  }
}

function endOfFile(checkdata){
  eofTime='15:59:59'
  if(companyName!=null){
    if(previousTime != eofTime){
      checkdata.lastTime=timeIncrement(eofTime)
      pushUpdatedDate(timeDifference(checkdata))
    }
    writeFile(companyName)
  }
  previousTime=null
  companyName=checkdata.ticker
}

function timeIncrement(time){
  intTime = time.split(':')
  hour = parseInt(intTime[0]);
  minute = parseInt(intTime[1])+1;
  hour = hour + Math.floor(minute/60);
  hour = hour%24
  second = parseInt(intTime[2]);
  minute = minute + Math.floor(second/60);
  minute = minute%60;
  second = second%60;
  checktime = hour+':'+minute+':'+second
  return(checktime)
}

function timeDifference(checkdata){
  eofTime=(('lastTime' in checkdata)?checkdata.lastTime:checkdata.time)
  dummy_date='2019-04-17 '
  startTime=previousTime
  updateData = previousData
  updateVolume = previoueVolume
  previousTime = new Date(dummy_date+previousTime)
  currentTime = new Date(dummy_date+eofTime)
  timeGap=(((currentTime - previousTime)/60000)-1)//60000 second to minute conversion
  previousTime=checkdata.time
  previousData = checkdata.close
  previoueVolume = checkdata.volume
  return{
    res:false,
    timeGap:timeGap,
    closeData:updateData,
    volume:updateVolume,
    startTime:startTime
  }
}

function updateFile(newData,writeData,nextTime){
  updateData={
    ticker: companyName,
    date: writeData.date,
    time: nextTime,
    open: newData.closeData,
    high: newData.closeData,
    low: newData.closeData,
    close: newData.closeData,
    volume: newData.volume,
    openInterest: writeData.openInterest
  }
  return(updateData)
}

function writeFile(fileName){
  csvWriter = createCsvWriter({
    path: './file3/'+fileName+'.csv',
    header: [
    {id: 'ticker', title: 'Ticker'},
    {id: 'date', title: 'Date'},
    {id: 'time', title: 'Time'},
    {id: 'open', title: 'Open'},
    {id: 'high', title: 'High'},
    {id: 'low', title: 'Low'},
    {id: 'close', title: 'Close'},
    {id: 'volume', title: 'Volume'},
    {id: 'openInterest', title: 'Open Interest'}
    ]
  });
  csvWriter.writeRecords(fileDataHash)
  fileDataHash = []
}

execute()

