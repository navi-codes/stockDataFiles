const fs = require('fs')
const csv = require('fast-csv')
const _ = require('lodash');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
companyName=null
previousTime=null
fileDataHash = []

function execute(){
  fileName = 'file2'
  fs.createReadStream(fileName+'.csv')
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
  pushUpdatedData(newData)
  fileDataHash.push(writeData)
}

function pushUpdatedData(newData){
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
    startOfFile(checkdata)
  }
  if(previousTime==null){
    previousTime=checkdata.time
    previousData = checkdata.close
    previoueVolume = checkdata.volume
    return{res:true}
  }else{
    checkTime = timeIncrement(previousTime)
  }
  if(checkdata.time==checkTime){
    previousTime=checkdata.time
    previousData = checkdata.close
    previoueVolume = checkdata.volume
    return{res:true}
  }else{
    return(timeDifference(checkdata))
  }
}

function endOfFile(checkdata){
  closeTime='15:59:59'
  if(companyName!=null){
    if(previousTime != closeTime){
      checkdata.checkTime=timeIncrement(closeTime)
      pushUpdatedData(timeDifference(checkdata))
    }
    writeFile(companyName)
  }
  previousTime=null
  companyName=checkdata.ticker
}

function startOfFile(checkdata){
  openTime='09:15:59'
  if(companyName!=null){
    if(checkdata.time != openTime){
      checkdata.checkTime = '09:14:59'
      checkdata.start=true
      pushUpdatedData(timeDifference(checkdata))
    }
  }
}

function timeIncrement(time){
  inTime = new Date('2019-04-22 '+time);
  inTime.setMinutes( inTime.getMinutes() +1 );
  incrementedTime = (inTime.getHours()+':'+inTime.getMinutes()+':'+inTime.getSeconds())
  return(incrementedTime)
}

function timeDifference(checkdata){
  inTime=(('checkTime' in checkdata)?checkdata.checkTime:checkdata.time)
  dummy_date='2019-04-17 '
  startTime=(('start' in checkdata) ? inTime:previousTime)
  updateData =(('start' in checkdata)?checkdata.open:previousData)
  updateVolume =(('start' in checkdata)?checkdata.volume:previoueVolume)
  previousTime = new Date(dummy_date+previousTime)
  currentTime = new Date(dummy_date+inTime)
  timeGap=(Math.abs((currentTime - previousTime)/60000)-1)
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
  isAppend=fs.existsSync('./splitfiles/'+companyName+'.csv')
  csvWriter = createCsvWriter({
    path: './splitfiles/'+fileName+'.csv',
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
    ],
    append:isAppend
  });
  csvWriter.writeRecords(fileDataHash)
  fileDataHash = []
}

execute()

