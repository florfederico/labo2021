rm( list=ls() )
gc()

#install.packages("sqldf")
#library(sqldf)
library("dplyr")
library("data.table")
library("rpart")
library("rpart.plot")


setwd("C:/Users/feder/OneDrive/Maestría Big Data/Laboratorio de Implementacion I/DM_EyF/" )  #establezco la carpeta donde voy a trabajar


#cargo el dataset
dtrain  <- fread( "./datasetsOri/datasetsOri_paquete_premium.csv")

dist_meses <- distinct(dtrain, foto_mes)
dist_meses <- dist_meses[with(dist_meses, order(dist_meses$foto_mes)), ]

meses <- as.vector(dist_meses$foto_mes)

meses

dapply <- select(subset(dtrain, foto_mes == meses[37]),numero_de_cliente)

dataset_hori <- subset(dtrain, foto_mes == meses[32])

for( i in 33:34) {
  dataset_aux <- subset(dtrain, foto_mes == meses[31])

  dataset_hori <-merge(x = dataset_hori, y = dataset_aux, 
                        by = "numero_de_cliente", all = TRUE,
                        suffixes=c(paste0(".",i), paste0(".",i+1)))
}

dataset_hori <- select(dataset_hori, -clase_ternaria.33, -clase_ternaria.34) 

dataset_hori <-merge(x = dapply, y = dataset_hori, 
                by = "numero_de_cliente")

#dataset_hori <- mutate_at(dataset_hori, c("clase_ternaria"), ~replace(., is.na(.), 0))
#dataset_hori[is.na(dataset_hori$clase_ternaria)] <- 'BAJA'
#dataset_hori[ , clase01:= ifelse( clase_ternaria=="CONTINUA", 0, 1 ) ]

dapply_hori <- subset(dtrain, foto_mes == meses[35])

for( i in 36:37) {
  dapply_aux <- subset(dtrain, foto_mes == meses[i])
  
  dapply_hori <-merge(x = dapply_hori, y = dapply_aux, 
                       by = "numero_de_cliente", all = TRUE,
                       suffixes=c(paste0(".",i-3), paste0(".",i-2)))
  
  
}

dapply_hori <- select(dapply_hori, -clase_ternaria.33, -clase_ternaria.34) 

dapply_hori <-merge(x = dapply, y = dapply_hori, 
                     by = "numero_de_cliente")


#uso esta semilla para los canaritos
set.seed(10219)
#agrego  30 canaritos
for( i in 1:95)  dataset_hori[ , paste0("canarito", i ) :=  runif( nrow(dataset_hori))]
for( i in 1:95)  dapply_hori[ , paste0("canarito", i ) :=  runif( nrow(dapply_hori))]

head(dataset_hori)

#Genero un arbol sin limite
modelo_original  <- rpart(formula= "clase_ternaria ~ . -mactivos_margen -mpasivos_margen",
                          data= dataset_hori,
                          xval= 0,
                          model= TRUE,
                          cp=        -1,
                          maxdepth=  30,  #lo dejo crecer lo mas posible
                          minsplit=   2,
                          minbucket=  1 )


#hago el pruning de los canaritos
#haciendo un hackeo a la estructura  modelo_original$frame
# -666 es un valor arbritrariamente negativo que jamas es generado por rpart
modelo_original$frame[ modelo_original$frame$var %like% "canarito", "complexity"] <- -666
modelo_pruned  <- prune(  modelo_original, -666 )


prediccion  <- predict( modelo_pruned, dapply_hori, type = "prob")[,"BAJA+2"]

entrega  <-  as.data.table( list( "numero_de_cliente"= dapply_hori$numero_de_cliente,
                                  "Predicted"= as.integer(  prediccion > 0.025 ) ) )

# entrega <-merge(x = dapply, y = entrega, 
#                     by = "numero_de_cliente")


fwrite( entrega, paste0( "./kaggle/canaritos_3meses.csv"), sep="," ) 
