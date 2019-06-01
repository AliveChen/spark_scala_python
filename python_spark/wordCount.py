from pyspark import SparkContext
from pyspark import SparkConf


def CreateSparkContext():
    sparkConf=SparkConf().setAppName('wordCount').set('spark.ui.show.Console.progress','false')
    sc=SparkContext(conf=sparkConf)
    print('master='+sc.master)
    #setLogger(sc)
    SetPath(sc)
    return(sc)
def SetPath(sc):
    global Path
    if sc.master[0:5]=='local':
        Path='file:/home/neo/pythonwork/pythonproject'
    else:
        Path='hdfs://matrix:9000/usr/neo/'
if __name__=='__main__':
    print('start runworkcount')
    sc=CreateSparkContext()
    print('loading data...')
    print(Path+'data/README.md')
    textFile=sc.textFile(Path+'/data/README.md')
    print('total'+str(textFile.count())+'rows')
    
    