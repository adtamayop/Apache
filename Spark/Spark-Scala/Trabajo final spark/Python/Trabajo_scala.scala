//lectura del archivo
var trackfile = sc.textFile("C:/spark/trabajo/tracks.csv")

//split cada fila para convertirlo a clave valor, donde la clave es el customerID
// y el valor es el resto de los atributos 
// "junto" todos los registros de cada customerID
var tbycust  = trackfile.map(line => (line.split(",")(1), 
                    List(line.split(",")(2),
                    line.split(",")(3),
                    line.split(",")(4),
                    line.split(",")(5)
                    ))).groupByKey()

/* PREGUNTARLE AL PROFE SI TENGO QUE UTILIZAR LOS MISMOS COMANDOS QUE 
SE UTILIZARON EN PYTHON O ES LLEGAR A LA RESPUESTA, OBVIAMENTE UTILIZANDO 
SPARK */

 // var x = tracks.mapValues(b=> b.mkString(": ")+",")
 // val tbycust = x.reduceByKey((x, y) => x + y)
 //  var tbycust = x.reduceByKey((x, y) => x + y)

// "junto" todos los registros de cada customerID
//var tbycust = tracks.groupByKey()


//duda con el return de la función es que no tiene sentido que el return de la función esté dentro del for, 
// porque así no va a acumular, entonces cual es el sentido de esas variables


def compute_stats_byuser(tracks:Iterable[List[String]]): List[Int] = {   

    var mcount = 0
    var morn = 0
    var aft = 0
    var eve = 0
    var night = 0    

    var tracklist = List[Integer]()

    for(t <- tracks){
        
        var trackid = t(0).toInt
        var dtime = t(1)
        var mobile = t(2).toInt
        var zip = t(3)

        if(!(tracklist.contains(trackid))){
            tracklist = trackid :: tracklist   
        }
        
        var fecha = dtime.split(" ")(0)
        var hora = dtime.split(" ")(1)

        var hora_dia = hora.split(":")(0).toInt
        mcount += mobile

        if(hora_dia < 5){
            night += 1
        } else if (hora_dia < 12) {
            morn += 1
        } else if (hora_dia < 17) {
            aft += 1
        } else if (hora_dia < 22) {
            eve += 1
        }else {
            night += 1
        }
   }

   val retorno = List(tracklist.length,morn,aft,eve,night,mcount)
   return retorno
}

var cusdata = tbycust.mapValues(compute_stats_byuser)

/////////////////////////////////////////////////////////////////////////////////////////////


import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}


//var lista = Array(("2828",List(125, 38, 26, 38, 32, 65)), ("2350",List(135, 44, 27, 31, 39, 69)), ("3492",List(97, 30, 26, 14, 30, 72)), ("1036",List(201, 50, 47, 50, 70, 104)), ("736",List(226, 79, 49, 60, 71, 123)))

def estadistica(entrada: Array[(String, List[Int])]): List[Double] ={
    
    //es necesario hacer la conversion de ese tipo array con el que viene cusdata a lista
    var lista = entrada.toList

    //para entrar al metodo colStats debe ser estrictamente de esta clase Vector
    var lista_estad = List[org.apache.spark.mllib.linalg.Vector]()

    for(i <- lista){

        var elementos = i._2

        var lista_double = elementos.map(_.toDouble)

        //deben de ser de tipo double para para ser del tipo vector dense
        lista_estad = Vectors.dense(lista_double.toArray) :: lista_estad
    }

    val observations = sc.parallelize(lista_estad)
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)

    return List(summary.mean(0),summary.mean(1),summary.mean(2),summary.mean(3),summary.mean(4),summary.mean(5))
}

//preguntarle también al profe que si le puedo pasar el parametro así a la función o no puedo utilizar el collect
var aggdata = estadistica(cusdata.collect)

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var lista = cusdata.collect
import java.io._
val pw = new PrintWriter(new File("live_table.csv"))

for(i <- lista){
    var x = i._2.mkString(",").toString
    pw.write(x + "\n")
}
pw.close

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

val pw = new PrintWriter(new File("agg_table.csv"))
var x = aggdata.mkString(",").toString
pw.write(x)
pw.close






