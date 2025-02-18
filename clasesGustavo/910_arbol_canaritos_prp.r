#limpio la memoria
rm( list=ls() )
gc()

library("data.table")
library("rpart")
library("rpart.plot")

setwd("C:/Users/feder/OneDrive/Maestr�a Big Data/Laboratorio de Implementacion I/DM_EyF/" )  #establezco la carpeta donde voy a trabajar
#cargo el dataset
dataset  <- fread( "./datasetsOri/paquete_premium_202011.csv")


#uso esta semilla para los canaritos
set.seed(102191)

#agrego una variable canarito, random distribucion uniforme en el intervalo [0,1]
dataset[ ,  canarito1 :=  runif( nrow(dataset) ) ]

#agrego los siguientes canaritos
for( i in 13:30 ) dataset[ , paste0("canarito", i ) :=  runif( nrow(dataset)) ]


#Primero  veo como quedan mis arboles
modelo  <- rpart(formula= "clase_ternaria ~ . -mactivos_margen -mpasivos_margen",
                 data= dataset[,],
                 model= TRUE,
                 xval= 0,
                 cp= 0, 
                 minsplit= 10, 
                 maxdepth= 10)


pdf(file = "./work/arbol_canaritos.pdf", width=28, height=4)
prp(modelo, extra=101, digits=5, branch=1, type=4, varlen=0, faclen=0)
dev.off()

