#Necesita para correr en Google Cloud
#64 GB de memoria RAM
#256 GB de espacio en el disco local
#8 vCPU


#Feature Engineering
#creo nuevas variables dentro del mismo mes
#Condimentar a gusto con nuevas variables

#limpio la memoria
rm( list=ls() )
gc()

require("data.table")



#Establezco el Working Directory
setwd( "~/buckets/b1/" )


EnriquecerDataset <- function( dataset , arch_destino )
{
  columnas_originales <-  copy(colnames( dataset ))

  #INICIO de la seccion donde se deben hacer cambios con variables nuevas
  #se crean los nuevos campos para MasterCard  y Visa, teniendo en cuenta los NA's
  #varias formas de combinar Visa_status y Master_status
  dataset[ , mv_status01       := pmax( Master_status,  Visa_status, na.rm = TRUE) ]
  dataset[ , mv_status02       := Master_status +  Visa_status ]
  dataset[ , mv_status03       := pmax( ifelse( is.na(Master_status), 10, Master_status) , ifelse( is.na(Visa_status), 10, Visa_status) ) ]
  dataset[ , mv_status04       := ifelse( is.na(Master_status), 10, Master_status)  +  ifelse( is.na(Visa_status), 10, Visa_status)  ]
  dataset[ , mv_status05       := ifelse( is.na(Master_status), 10, Master_status)  +  100*ifelse( is.na(Visa_status), 10, Visa_status)  ]

  dataset[ , mv_status06       := ifelse( is.na(Visa_status), 
                                          ifelse( is.na(Master_status), 10, Master_status), 
                                          Visa_status)  ]

  dataset[ , mv_status07       := ifelse( is.na(Master_status), 
                                          ifelse( is.na(Visa_status), 10, Visa_status), 
                                          Master_status)  ]


  #combino MasterCard y Visa
  dataset[ , mv_mfinanciacion_limite := rowSums( cbind( Master_mfinanciacion_limite,  Visa_mfinanciacion_limite) , na.rm=TRUE ) ]

  dataset[ , mv_Fvencimiento         := pmin( Master_Fvencimiento, Visa_Fvencimiento, na.rm = TRUE) ]
  dataset[ , mv_Finiciomora          := pmin( Master_Finiciomora, Visa_Finiciomora, na.rm = TRUE) ]
  dataset[ , mv_msaldototal          := rowSums( cbind( Master_msaldototal,  Visa_msaldototal) , na.rm=TRUE ) ]
  dataset[ , mv_msaldopesos          := rowSums( cbind( Master_msaldopesos,  Visa_msaldopesos) , na.rm=TRUE ) ]
  dataset[ , mv_msaldodolares        := rowSums( cbind( Master_msaldodolares,  Visa_msaldodolares) , na.rm=TRUE ) ]
  dataset[ , mv_mconsumospesos       := rowSums( cbind( Master_mconsumospesos,  Visa_mconsumospesos) , na.rm=TRUE ) ]
  dataset[ , mv_mconsumosdolares     := rowSums( cbind( Master_mconsumosdolares,  Visa_mconsumosdolares) , na.rm=TRUE ) ]
  dataset[ , mv_mlimitecompra        := rowSums( cbind( Master_mlimitecompra,  Visa_mlimitecompra) , na.rm=TRUE ) ]
  dataset[ , mv_madelantopesos       := rowSums( cbind( Master_madelantopesos,  Visa_madelantopesos) , na.rm=TRUE ) ]
  dataset[ , mv_madelantodolares     := rowSums( cbind( Master_madelantodolares,  Visa_madelantodolares) , na.rm=TRUE ) ]
  dataset[ , mv_fultimo_cierre       := pmax( Master_fultimo_cierre, Visa_fultimo_cierre, na.rm = TRUE) ]
  dataset[ , mv_mpagado              := rowSums( cbind( Master_mpagado,  Visa_mpagado) , na.rm=TRUE ) ]
  dataset[ , mv_mpagospesos          := rowSums( cbind( Master_mpagospesos,  Visa_mpagospesos) , na.rm=TRUE ) ]
  dataset[ , mv_mpagosdolares        := rowSums( cbind( Master_mpagosdolares,  Visa_mpagosdolares) , na.rm=TRUE ) ]
  dataset[ , mv_fechaalta            := pmax( Master_fechaalta, Visa_fechaalta, na.rm = TRUE) ]
  dataset[ , mv_mconsumototal        := rowSums( cbind( Master_mconsumototal,  Visa_mconsumototal) , na.rm=TRUE ) ]
  dataset[ , mv_cconsumos            := rowSums( cbind( Master_cconsumos,  Visa_cconsumos) , na.rm=TRUE ) ]
  dataset[ , mv_cadelantosefectivo   := rowSums( cbind( Master_cadelantosefectivo,  Visa_cadelantosefectivo) , na.rm=TRUE ) ]
  dataset[ , mv_mpagominimo          := rowSums( cbind( Master_mpagominimo,  Visa_mpagominimo) , na.rm=TRUE ) ]

  #a partir de aqui juego con la suma de Mastercard y Visa
  dataset[ , mvr_Master_mlimitecompra:= Master_mlimitecompra / mv_mlimitecompra ]
  dataset[ , mvr_Visa_mlimitecompra  := Visa_mlimitecompra / mv_mlimitecompra ]
  dataset[ , mvr_msaldototal         := mv_msaldototal / mv_mlimitecompra ]
  dataset[ , mvr_msaldopesos         := mv_msaldopesos / mv_mlimitecompra ]
  dataset[ , mvr_msaldopesos2        := mv_msaldopesos / mv_msaldototal ]
  dataset[ , mvr_msaldodolares       := mv_msaldodolares / mv_mlimitecompra ]
  dataset[ , mvr_msaldodolares2      := mv_msaldodolares / mv_msaldototal ]
  dataset[ , mvr_mconsumospesos      := mv_mconsumospesos / mv_mlimitecompra ]
  dataset[ , mvr_mconsumosdolares    := mv_mconsumosdolares / mv_mlimitecompra ]
  dataset[ , mvr_madelantopesos      := mv_madelantopesos / mv_mlimitecompra ]
  dataset[ , mvr_madelantodolares    := mv_madelantodolares / mv_mlimitecompra ]
  dataset[ , mvr_mpagado             := mv_mpagado / mv_mlimitecompra ]
  dataset[ , mvr_mpagospesos         := mv_mpagospesos / mv_mlimitecompra ]
  dataset[ , mvr_mpagosdolares       := mv_mpagosdolares / mv_mlimitecompra ]
  dataset[ , mvr_mconsumototal       := mv_mconsumototal  / mv_mlimitecompra ]
  dataset[ , mvr_mpagominimo         := mv_mpagominimo  / mv_mlimitecompra ]
  
  
dataset[ , extra_1 := ctrx_quarter/ctrx_quarter]
dataset[ , extra_2 := ctrx_quarter/mcaja_ahorro]
dataset[ , extra_3 := ctrx_quarter/cpayroll_trx]
dataset[ , extra_4 := ctrx_quarter/mcuentas_saldo]
dataset[ , extra_5 := ctrx_quarter/ctarjeta_visa_transacciones]
dataset[ , extra_6 := ctrx_quarter/mpayroll]
dataset[ , extra_7 := ctrx_quarter/mprestamos_personales]
dataset[ , extra_8 := ctrx_quarter/mtarjeta_visa_consumo]
dataset[ , extra_9 := ctrx_quarter/mactivos_margen]
dataset[ , extra_10 := ctrx_quarter/Visa_mconsumototal]
dataset[ , extra_11 := ctrx_quarter/Visa_msaldototal]
dataset[ , extra_12 := ctrx_quarter/Visa_mpagominimo]
dataset[ , extra_13 := ctrx_quarter/Visa_msaldopesos]
dataset[ , extra_14 := mcaja_ahorro/ctrx_quarter]
dataset[ , extra_15 := mcaja_ahorro/mcaja_ahorro]
dataset[ , extra_16 := mcaja_ahorro/cpayroll_trx]
dataset[ , extra_17 := mcaja_ahorro/mcuentas_saldo]
dataset[ , extra_18 := mcaja_ahorro/ctarjeta_visa_transacciones]
dataset[ , extra_19 := mcaja_ahorro/mpayroll]
dataset[ , extra_20 := mcaja_ahorro/mprestamos_personales]
dataset[ , extra_21 := mcaja_ahorro/mtarjeta_visa_consumo]
dataset[ , extra_22 := mcaja_ahorro/mactivos_margen]
dataset[ , extra_23 := mcaja_ahorro/Visa_mconsumototal]
dataset[ , extra_24 := mcaja_ahorro/Visa_msaldototal]
dataset[ , extra_25 := mcaja_ahorro/Visa_mpagominimo]
dataset[ , extra_26 := mcaja_ahorro/Visa_msaldopesos]
dataset[ , extra_27 := cpayroll_trx/ctrx_quarter]
dataset[ , extra_28 := cpayroll_trx/mcaja_ahorro]
dataset[ , extra_29 := cpayroll_trx/cpayroll_trx]
dataset[ , extra_30 := cpayroll_trx/mcuentas_saldo]
dataset[ , extra_31 := cpayroll_trx/ctarjeta_visa_transacciones]
dataset[ , extra_32 := cpayroll_trx/mpayroll]
dataset[ , extra_33 := cpayroll_trx/mprestamos_personales]
dataset[ , extra_34 := cpayroll_trx/mtarjeta_visa_consumo]
dataset[ , extra_35 := cpayroll_trx/mactivos_margen]
dataset[ , extra_36 := cpayroll_trx/Visa_mconsumototal]
dataset[ , extra_37 := cpayroll_trx/Visa_msaldototal]
dataset[ , extra_38 := cpayroll_trx/Visa_mpagominimo]
dataset[ , extra_39 := cpayroll_trx/Visa_msaldopesos]
dataset[ , extra_40 := mcuentas_saldo/ctrx_quarter]
dataset[ , extra_41 := mcuentas_saldo/mcaja_ahorro]
dataset[ , extra_42 := mcuentas_saldo/cpayroll_trx]
dataset[ , extra_43 := mcuentas_saldo/mcuentas_saldo]
dataset[ , extra_44 := mcuentas_saldo/ctarjeta_visa_transacciones]
dataset[ , extra_45 := mcuentas_saldo/mpayroll]
dataset[ , extra_46 := mcuentas_saldo/mprestamos_personales]
dataset[ , extra_47 := mcuentas_saldo/mtarjeta_visa_consumo]
dataset[ , extra_48 := mcuentas_saldo/mactivos_margen]
dataset[ , extra_49 := mcuentas_saldo/Visa_mconsumototal]
dataset[ , extra_50 := mcuentas_saldo/Visa_msaldototal]
dataset[ , extra_51 := mcuentas_saldo/Visa_mpagominimo]
dataset[ , extra_52 := mcuentas_saldo/Visa_msaldopesos]
dataset[ , extra_53 := ctarjeta_visa_transacciones/ctrx_quarter]
dataset[ , extra_54 := ctarjeta_visa_transacciones/mcaja_ahorro]
dataset[ , extra_55 := ctarjeta_visa_transacciones/cpayroll_trx]
dataset[ , extra_56 := ctarjeta_visa_transacciones/mcuentas_saldo]
dataset[ , extra_57 := ctarjeta_visa_transacciones/ctarjeta_visa_transacciones]
dataset[ , extra_58 := ctarjeta_visa_transacciones/mpayroll]
dataset[ , extra_59 := ctarjeta_visa_transacciones/mprestamos_personales]
dataset[ , extra_60 := ctarjeta_visa_transacciones/mtarjeta_visa_consumo]
dataset[ , extra_61 := ctarjeta_visa_transacciones/mactivos_margen]
dataset[ , extra_62 := ctarjeta_visa_transacciones/Visa_mconsumototal]
dataset[ , extra_63 := ctarjeta_visa_transacciones/Visa_msaldototal]
dataset[ , extra_64 := ctarjeta_visa_transacciones/Visa_mpagominimo]
dataset[ , extra_65 := ctarjeta_visa_transacciones/Visa_msaldopesos]
dataset[ , extra_66 := mpayroll/ctrx_quarter]
dataset[ , extra_67 := mpayroll/mcaja_ahorro]
dataset[ , extra_68 := mpayroll/cpayroll_trx]
dataset[ , extra_69 := mpayroll/mcuentas_saldo]
dataset[ , extra_70 := mpayroll/ctarjeta_visa_transacciones]
dataset[ , extra_71 := mpayroll/mpayroll]
dataset[ , extra_72 := mpayroll/mprestamos_personales]
dataset[ , extra_73 := mpayroll/mtarjeta_visa_consumo]
dataset[ , extra_74 := mpayroll/mactivos_margen]
dataset[ , extra_75 := mpayroll/Visa_mconsumototal]
dataset[ , extra_76 := mpayroll/Visa_msaldototal]
dataset[ , extra_77 := mpayroll/Visa_mpagominimo]
dataset[ , extra_78 := mpayroll/Visa_msaldopesos]
dataset[ , extra_79 := mprestamos_personales/ctrx_quarter]
dataset[ , extra_80 := mprestamos_personales/mcaja_ahorro]
dataset[ , extra_81 := mprestamos_personales/cpayroll_trx]
dataset[ , extra_82 := mprestamos_personales/mcuentas_saldo]
dataset[ , extra_83 := mprestamos_personales/ctarjeta_visa_transacciones]
dataset[ , extra_84 := mprestamos_personales/mpayroll]
dataset[ , extra_85 := mprestamos_personales/mprestamos_personales]
dataset[ , extra_86 := mprestamos_personales/mtarjeta_visa_consumo]
dataset[ , extra_87 := mprestamos_personales/mactivos_margen]
dataset[ , extra_88 := mprestamos_personales/Visa_mconsumototal]
dataset[ , extra_89 := mprestamos_personales/Visa_msaldototal]
dataset[ , extra_90 := mprestamos_personales/Visa_mpagominimo]
dataset[ , extra_91 := mprestamos_personales/Visa_msaldopesos]
dataset[ , extra_92 := mtarjeta_visa_consumo/ctrx_quarter]
dataset[ , extra_93 := mtarjeta_visa_consumo/mcaja_ahorro]
dataset[ , extra_94 := mtarjeta_visa_consumo/cpayroll_trx]
dataset[ , extra_95 := mtarjeta_visa_consumo/mcuentas_saldo]
dataset[ , extra_96 := mtarjeta_visa_consumo/ctarjeta_visa_transacciones]
dataset[ , extra_97 := mtarjeta_visa_consumo/mpayroll]
dataset[ , extra_98 := mtarjeta_visa_consumo/mprestamos_personales]
dataset[ , extra_99 := mtarjeta_visa_consumo/mtarjeta_visa_consumo]
dataset[ , extra_100 := mtarjeta_visa_consumo/mactivos_margen]
dataset[ , extra_101 := mtarjeta_visa_consumo/Visa_mconsumototal]
dataset[ , extra_102 := mtarjeta_visa_consumo/Visa_msaldototal]
dataset[ , extra_103 := mtarjeta_visa_consumo/Visa_mpagominimo]
dataset[ , extra_104 := mtarjeta_visa_consumo/Visa_msaldopesos]
dataset[ , extra_105 := mactivos_margen/ctrx_quarter]
dataset[ , extra_106 := mactivos_margen/mcaja_ahorro]
dataset[ , extra_107 := mactivos_margen/cpayroll_trx]
dataset[ , extra_108 := mactivos_margen/mcuentas_saldo]
dataset[ , extra_109 := mactivos_margen/ctarjeta_visa_transacciones]
dataset[ , extra_110 := mactivos_margen/mpayroll]
dataset[ , extra_111 := mactivos_margen/mprestamos_personales]
dataset[ , extra_112 := mactivos_margen/mtarjeta_visa_consumo]
dataset[ , extra_113 := mactivos_margen/mactivos_margen]
dataset[ , extra_114 := mactivos_margen/Visa_mconsumototal]
dataset[ , extra_115 := mactivos_margen/Visa_msaldototal]
dataset[ , extra_116 := mactivos_margen/Visa_mpagominimo]
dataset[ , extra_117 := mactivos_margen/Visa_msaldopesos]
dataset[ , extra_118 := mcuenta_debitos_automaticos/ctrx_quarter]
dataset[ , extra_119 := mcuenta_debitos_automaticos/mcaja_ahorro]
dataset[ , extra_120 := mcuenta_debitos_automaticos/cpayroll_trx]
dataset[ , extra_121 := mcuenta_debitos_automaticos/mcuentas_saldo]
dataset[ , extra_122 := mcuenta_debitos_automaticos/ctarjeta_visa_transacciones]
dataset[ , extra_123 := mcuenta_debitos_automaticos/mpayroll]
dataset[ , extra_124 := mcuenta_debitos_automaticos/mprestamos_personales]
dataset[ , extra_125 := mcuenta_debitos_automaticos/mtarjeta_visa_consumo]
dataset[ , extra_126 := mcuenta_debitos_automaticos/mactivos_margen]
dataset[ , extra_127 := mcuenta_debitos_automaticos/Visa_mconsumototal]
dataset[ , extra_128 := mcuenta_debitos_automaticos/Visa_msaldototal]
dataset[ , extra_129 := mcuenta_debitos_automaticos/Visa_mpagominimo]
dataset[ , extra_130 := mcuenta_debitos_automaticos/Visa_msaldopesos]
dataset[ , extra_131 := mcuenta_debitos_automaticos/mpasivos_margen]
dataset[ , extra_132 := mcuenta_debitos_automaticos/Visa_fechaalta]
dataset[ , extra_133 := mcuenta_debitos_automaticos/thomebanking]
dataset[ , extra_134 := mcuenta_debitos_automaticos/mcuenta_debitos_automaticos]
dataset[ , extra_135 := mcuenta_debitos_automaticos/cproductos]
dataset[ , extra_136 := mcuenta_debitos_automaticos/chomebanking_transacciones]
dataset[ , extra_137 := mcuenta_debitos_automaticos/Master_status]
dataset[ , extra_138 := mcuenta_debitos_automaticos/Master_fechaalta]
dataset[ , extra_139 := mcuenta_debitos_automaticos/mcomisiones_mantenimiento]
dataset[ , extra_140 := mcuenta_debitos_automaticos/mrentabilidad_annual]
dataset[ , extra_141 := mcuenta_debitos_automaticos/mrentabilidad]
  

  #valvula de seguridad para evitar valores infinitos
  #paso los infinitos a NULOS
  infinitos      <- lapply(names(dataset),function(.name) dataset[ , sum(is.infinite(get(.name)))])
  infinitos_qty  <- sum( unlist( infinitos) )
  if( infinitos_qty > 0 )
  {
    cat( "ATENCION, hay", infinitos_qty, "valores infinitos en tu dataset. Seran pasados a NA\n" )
    dataset[mapply(is.infinite, dataset)] <- NA
  }


  #valvula de seguridad para evitar valores NaN  que es 0/0
  #paso los NaN a 0 , decision polemica si las hay
  #se invita a asignar un valor razonable segun la semantica del campo creado
  nans      <- lapply(names(dataset),function(.name) dataset[ , sum(is.nan(get(.name)))])
  nans_qty  <- sum( unlist( nans) )
  if( nans_qty > 0 )
  {
    cat( "ATENCION, hay", nans_qty, "valores NaN 0/0 en tu dataset. Seran pasados arbitrariamente a 0\n" )
    cat( "Si no te gusta la decision, modifica a gusto el programa!\n\n")
    dataset[mapply(is.nan, dataset)] <- 0
  }

  #FIN de la seccion donde se deben hacer cambios con variables nuevas

  columnas_extendidas <-  copy( setdiff(  colnames(dataset), columnas_originales ) )

  #grabo con nombre extendido
  fwrite( dataset,
          file=arch_destino,
          sep= "," )
}
#------------------------------------------------------------------------------

dir.create( "./datasets/" )

#lectura rapida del dataset  usando fread  de la libreria  data.table
dataset1  <- fread("./datasetsOri/paquete_premium.csv.gz")

EnriquecerDataset( dataset1, "./datasets/paquete_premium_ext.csv.gz" )

quit( save="no")
