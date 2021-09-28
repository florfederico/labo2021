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
  
  
  dataset[ , extra_1 := ctrx_quarter_lag1/ctrx_quarter_lag1]
dataset[ , extra_2 := ctrx_quarter_lag1/ctrx_quarter]
dataset[ , extra_3 := ctrx_quarter_lag1/mcaja_ahorro]
dataset[ , extra_4 := ctrx_quarter_lag1/cpayroll_trx]
dataset[ , extra_5 := ctrx_quarter_lag1/mcuentas_saldo]
dataset[ , extra_6 := ctrx_quarter_lag1/ctarjeta_visa_transacciones]
dataset[ , extra_7 := ctrx_quarter_lag1/mpayroll]
dataset[ , extra_8 := ctrx_quarter_lag1/mprestamos_personales]
dataset[ , extra_9 := ctrx_quarter_lag1/mpayroll_lag1]
dataset[ , extra_10 := ctrx_quarter_lag1/mpasivos_margen_lag1]
dataset[ , extra_11 := ctrx_quarter_lag1/mtarjeta_visa_consumo]
dataset[ , extra_12 := ctrx_quarter_lag1/mactivos_margen]
dataset[ , extra_13 := ctrx_quarter_lag1/mcaja_ahorro_lag1]
dataset[ , extra_14 := ctrx_quarter_lag1/Visa_mconsumototal]
dataset[ , extra_15 := ctrx_quarter_lag1/Visa_mpagominimo_lag1]
dataset[ , extra_16 := ctrx_quarter_lag1/cpayroll_trx_lag1]
dataset[ , extra_17 := ctrx_quarter_lag1/Visa_msaldototal]
dataset[ , extra_18 := ctrx_quarter_lag1/Visa_mpagominimo]
dataset[ , extra_19 := ctrx_quarter_lag1/mactivos_margen_delta1]
dataset[ , extra_20 := ctrx_quarter_lag1/Visa_msaldopesos]
dataset[ , extra_21 := ctrx_quarter/ctrx_quarter_lag1]
dataset[ , extra_22 := ctrx_quarter/ctrx_quarter]
dataset[ , extra_23 := ctrx_quarter/mcaja_ahorro]
dataset[ , extra_24 := ctrx_quarter/cpayroll_trx]
dataset[ , extra_25 := ctrx_quarter/mcuentas_saldo]
dataset[ , extra_26 := ctrx_quarter/ctarjeta_visa_transacciones]
dataset[ , extra_27 := ctrx_quarter/mpayroll]
dataset[ , extra_28 := ctrx_quarter/mprestamos_personales]
dataset[ , extra_29 := ctrx_quarter/mpayroll_lag1]
dataset[ , extra_30 := ctrx_quarter/mpasivos_margen_lag1]
dataset[ , extra_31 := ctrx_quarter/mtarjeta_visa_consumo]
dataset[ , extra_32 := ctrx_quarter/mactivos_margen]
dataset[ , extra_33 := ctrx_quarter/mcaja_ahorro_lag1]
dataset[ , extra_34 := ctrx_quarter/Visa_mconsumototal]
dataset[ , extra_35 := ctrx_quarter/Visa_mpagominimo_lag1]
dataset[ , extra_36 := ctrx_quarter/cpayroll_trx_lag1]
dataset[ , extra_37 := ctrx_quarter/Visa_msaldototal]
dataset[ , extra_38 := ctrx_quarter/Visa_mpagominimo]
dataset[ , extra_39 := ctrx_quarter/mactivos_margen_delta1]
dataset[ , extra_40 := ctrx_quarter/Visa_msaldopesos]
dataset[ , extra_41 := mcaja_ahorro/ctrx_quarter_lag1]
dataset[ , extra_42 := mcaja_ahorro/ctrx_quarter]
dataset[ , extra_43 := mcaja_ahorro/mcaja_ahorro]
dataset[ , extra_44 := mcaja_ahorro/cpayroll_trx]
dataset[ , extra_45 := mcaja_ahorro/mcuentas_saldo]
dataset[ , extra_46 := mcaja_ahorro/ctarjeta_visa_transacciones]
dataset[ , extra_47 := mcaja_ahorro/mpayroll]
dataset[ , extra_48 := mcaja_ahorro/mprestamos_personales]
dataset[ , extra_49 := mcaja_ahorro/mpayroll_lag1]
dataset[ , extra_50 := mcaja_ahorro/mpasivos_margen_lag1]
dataset[ , extra_51 := mcaja_ahorro/mtarjeta_visa_consumo]
dataset[ , extra_52 := mcaja_ahorro/mactivos_margen]
dataset[ , extra_53 := mcaja_ahorro/mcaja_ahorro_lag1]
dataset[ , extra_54 := mcaja_ahorro/Visa_mconsumototal]
dataset[ , extra_55 := mcaja_ahorro/Visa_mpagominimo_lag1]
dataset[ , extra_56 := mcaja_ahorro/cpayroll_trx_lag1]
dataset[ , extra_57 := mcaja_ahorro/Visa_msaldototal]
dataset[ , extra_58 := mcaja_ahorro/Visa_mpagominimo]
dataset[ , extra_59 := mcaja_ahorro/mactivos_margen_delta1]
dataset[ , extra_60 := mcaja_ahorro/Visa_msaldopesos]
dataset[ , extra_61 := cpayroll_trx/ctrx_quarter_lag1]
dataset[ , extra_62 := cpayroll_trx/ctrx_quarter]
dataset[ , extra_63 := cpayroll_trx/mcaja_ahorro]
dataset[ , extra_64 := cpayroll_trx/cpayroll_trx]
dataset[ , extra_65 := cpayroll_trx/mcuentas_saldo]
dataset[ , extra_66 := cpayroll_trx/ctarjeta_visa_transacciones]
dataset[ , extra_67 := cpayroll_trx/mpayroll]
dataset[ , extra_68 := cpayroll_trx/mprestamos_personales]
dataset[ , extra_69 := cpayroll_trx/mpayroll_lag1]
dataset[ , extra_70 := cpayroll_trx/mpasivos_margen_lag1]
dataset[ , extra_71 := cpayroll_trx/mtarjeta_visa_consumo]
dataset[ , extra_72 := cpayroll_trx/mactivos_margen]
dataset[ , extra_73 := cpayroll_trx/mcaja_ahorro_lag1]
dataset[ , extra_74 := cpayroll_trx/Visa_mconsumototal]
dataset[ , extra_75 := cpayroll_trx/Visa_mpagominimo_lag1]
dataset[ , extra_76 := cpayroll_trx/cpayroll_trx_lag1]
dataset[ , extra_77 := cpayroll_trx/Visa_msaldototal]
dataset[ , extra_78 := cpayroll_trx/Visa_mpagominimo]
dataset[ , extra_79 := cpayroll_trx/mactivos_margen_delta1]
dataset[ , extra_80 := cpayroll_trx/Visa_msaldopesos]
dataset[ , extra_81 := mcuentas_saldo/ctrx_quarter_lag1]
dataset[ , extra_82 := mcuentas_saldo/ctrx_quarter]
dataset[ , extra_83 := mcuentas_saldo/mcaja_ahorro]
dataset[ , extra_84 := mcuentas_saldo/cpayroll_trx]
dataset[ , extra_85 := mcuentas_saldo/mcuentas_saldo]
dataset[ , extra_86 := mcuentas_saldo/ctarjeta_visa_transacciones]
dataset[ , extra_87 := mcuentas_saldo/mpayroll]
dataset[ , extra_88 := mcuentas_saldo/mprestamos_personales]
dataset[ , extra_89 := mcuentas_saldo/mpayroll_lag1]
dataset[ , extra_90 := mcuentas_saldo/mpasivos_margen_lag1]
dataset[ , extra_91 := mcuentas_saldo/mtarjeta_visa_consumo]
dataset[ , extra_92 := mcuentas_saldo/mactivos_margen]
dataset[ , extra_93 := mcuentas_saldo/mcaja_ahorro_lag1]
dataset[ , extra_94 := mcuentas_saldo/Visa_mconsumototal]
dataset[ , extra_95 := mcuentas_saldo/Visa_mpagominimo_lag1]
dataset[ , extra_96 := mcuentas_saldo/cpayroll_trx_lag1]
dataset[ , extra_97 := mcuentas_saldo/Visa_msaldototal]
dataset[ , extra_98 := mcuentas_saldo/Visa_mpagominimo]
dataset[ , extra_99 := mcuentas_saldo/mactivos_margen_delta1]
dataset[ , extra_100 := mcuentas_saldo/Visa_msaldopesos]
dataset[ , extra_101 := ctarjeta_visa_transacciones/ctrx_quarter_lag1]
dataset[ , extra_102 := ctarjeta_visa_transacciones/ctrx_quarter]
dataset[ , extra_103 := ctarjeta_visa_transacciones/mcaja_ahorro]
dataset[ , extra_104 := ctarjeta_visa_transacciones/cpayroll_trx]
dataset[ , extra_105 := ctarjeta_visa_transacciones/mcuentas_saldo]
dataset[ , extra_106 := ctarjeta_visa_transacciones/ctarjeta_visa_transacciones]
dataset[ , extra_107 := ctarjeta_visa_transacciones/mpayroll]
dataset[ , extra_108 := ctarjeta_visa_transacciones/mprestamos_personales]
dataset[ , extra_109 := ctarjeta_visa_transacciones/mpayroll_lag1]
dataset[ , extra_110 := ctarjeta_visa_transacciones/mpasivos_margen_lag1]
dataset[ , extra_111 := ctarjeta_visa_transacciones/mtarjeta_visa_consumo]
dataset[ , extra_112 := ctarjeta_visa_transacciones/mactivos_margen]
dataset[ , extra_113 := ctarjeta_visa_transacciones/mcaja_ahorro_lag1]
dataset[ , extra_114 := ctarjeta_visa_transacciones/Visa_mconsumototal]
dataset[ , extra_115 := ctarjeta_visa_transacciones/Visa_mpagominimo_lag1]
dataset[ , extra_116 := ctarjeta_visa_transacciones/cpayroll_trx_lag1]
dataset[ , extra_117 := ctarjeta_visa_transacciones/Visa_msaldototal]
dataset[ , extra_118 := ctarjeta_visa_transacciones/Visa_mpagominimo]
dataset[ , extra_119 := ctarjeta_visa_transacciones/mactivos_margen_delta1]
dataset[ , extra_120 := ctarjeta_visa_transacciones/Visa_msaldopesos]
dataset[ , extra_121 := mpayroll/ctrx_quarter_lag1]
dataset[ , extra_122 := mpayroll/ctrx_quarter]
dataset[ , extra_123 := mpayroll/mcaja_ahorro]
dataset[ , extra_124 := mpayroll/cpayroll_trx]
dataset[ , extra_125 := mpayroll/mcuentas_saldo]
dataset[ , extra_126 := mpayroll/ctarjeta_visa_transacciones]
dataset[ , extra_127 := mpayroll/mpayroll]
dataset[ , extra_128 := mpayroll/mprestamos_personales]
dataset[ , extra_129 := mpayroll/mpayroll_lag1]
dataset[ , extra_130 := mpayroll/mpasivos_margen_lag1]
dataset[ , extra_131 := mpayroll/mtarjeta_visa_consumo]
dataset[ , extra_132 := mpayroll/mactivos_margen]
dataset[ , extra_133 := mpayroll/mcaja_ahorro_lag1]
dataset[ , extra_134 := mpayroll/Visa_mconsumototal]
dataset[ , extra_135 := mpayroll/Visa_mpagominimo_lag1]
dataset[ , extra_136 := mpayroll/cpayroll_trx_lag1]
dataset[ , extra_137 := mpayroll/Visa_msaldototal]
dataset[ , extra_138 := mpayroll/Visa_mpagominimo]
dataset[ , extra_139 := mpayroll/mactivos_margen_delta1]
dataset[ , extra_140 := mpayroll/Visa_msaldopesos]
dataset[ , extra_141 := mprestamos_personales/ctrx_quarter_lag1]
dataset[ , extra_142 := mprestamos_personales/ctrx_quarter]
dataset[ , extra_143 := mprestamos_personales/mcaja_ahorro]
dataset[ , extra_144 := mprestamos_personales/cpayroll_trx]
dataset[ , extra_145 := mprestamos_personales/mcuentas_saldo]
dataset[ , extra_146 := mprestamos_personales/ctarjeta_visa_transacciones]
dataset[ , extra_147 := mprestamos_personales/mpayroll]
dataset[ , extra_148 := mprestamos_personales/mprestamos_personales]
dataset[ , extra_149 := mprestamos_personales/mpayroll_lag1]
dataset[ , extra_150 := mprestamos_personales/mpasivos_margen_lag1]
dataset[ , extra_151 := mprestamos_personales/mtarjeta_visa_consumo]
dataset[ , extra_152 := mprestamos_personales/mactivos_margen]
dataset[ , extra_153 := mprestamos_personales/mcaja_ahorro_lag1]
dataset[ , extra_154 := mprestamos_personales/Visa_mconsumototal]
dataset[ , extra_155 := mprestamos_personales/Visa_mpagominimo_lag1]
dataset[ , extra_156 := mprestamos_personales/cpayroll_trx_lag1]
dataset[ , extra_157 := mprestamos_personales/Visa_msaldototal]
dataset[ , extra_158 := mprestamos_personales/Visa_mpagominimo]
dataset[ , extra_159 := mprestamos_personales/mactivos_margen_delta1]
dataset[ , extra_160 := mprestamos_personales/Visa_msaldopesos]
dataset[ , extra_161 := mpayroll_lag1/ctrx_quarter_lag1]
dataset[ , extra_162 := mpayroll_lag1/ctrx_quarter]
dataset[ , extra_163 := mpayroll_lag1/mcaja_ahorro]
dataset[ , extra_164 := mpayroll_lag1/cpayroll_trx]
dataset[ , extra_165 := mpayroll_lag1/mcuentas_saldo]
dataset[ , extra_166 := mpayroll_lag1/ctarjeta_visa_transacciones]
dataset[ , extra_167 := mpayroll_lag1/mpayroll]
dataset[ , extra_168 := mpayroll_lag1/mprestamos_personales]
dataset[ , extra_169 := mpayroll_lag1/mpayroll_lag1]
dataset[ , extra_170 := mpayroll_lag1/mpasivos_margen_lag1]
dataset[ , extra_171 := mpayroll_lag1/mtarjeta_visa_consumo]
dataset[ , extra_172 := mpayroll_lag1/mactivos_margen]
dataset[ , extra_173 := mpayroll_lag1/mcaja_ahorro_lag1]
dataset[ , extra_174 := mpayroll_lag1/Visa_mconsumototal]
dataset[ , extra_175 := mpayroll_lag1/Visa_mpagominimo_lag1]
dataset[ , extra_176 := mpayroll_lag1/cpayroll_trx_lag1]
dataset[ , extra_177 := mpayroll_lag1/Visa_msaldototal]
dataset[ , extra_178 := mpayroll_lag1/Visa_mpagominimo]
dataset[ , extra_179 := mpayroll_lag1/mactivos_margen_delta1]
dataset[ , extra_180 := mpayroll_lag1/Visa_msaldopesos]
dataset[ , extra_181 := mpasivos_margen_lag1/ctrx_quarter_lag1]
dataset[ , extra_182 := mpasivos_margen_lag1/ctrx_quarter]
dataset[ , extra_183 := mpasivos_margen_lag1/mcaja_ahorro]
dataset[ , extra_184 := mpasivos_margen_lag1/cpayroll_trx]
dataset[ , extra_185 := mpasivos_margen_lag1/mcuentas_saldo]
dataset[ , extra_186 := mpasivos_margen_lag1/ctarjeta_visa_transacciones]
dataset[ , extra_187 := mpasivos_margen_lag1/mpayroll]
dataset[ , extra_188 := mpasivos_margen_lag1/mprestamos_personales]
dataset[ , extra_189 := mpasivos_margen_lag1/mpayroll_lag1]
dataset[ , extra_190 := mpasivos_margen_lag1/mpasivos_margen_lag1]
dataset[ , extra_191 := mpasivos_margen_lag1/mtarjeta_visa_consumo]
dataset[ , extra_192 := mpasivos_margen_lag1/mactivos_margen]
dataset[ , extra_193 := mpasivos_margen_lag1/mcaja_ahorro_lag1]
dataset[ , extra_194 := mpasivos_margen_lag1/Visa_mconsumototal]
dataset[ , extra_195 := mpasivos_margen_lag1/Visa_mpagominimo_lag1]
dataset[ , extra_196 := mpasivos_margen_lag1/cpayroll_trx_lag1]
dataset[ , extra_197 := mpasivos_margen_lag1/Visa_msaldototal]
dataset[ , extra_198 := mpasivos_margen_lag1/Visa_mpagominimo]
dataset[ , extra_199 := mpasivos_margen_lag1/mactivos_margen_delta1]
dataset[ , extra_200 := mpasivos_margen_lag1/Visa_msaldopesos]
dataset[ , extra_201 := mtarjeta_visa_consumo/ctrx_quarter_lag1]
dataset[ , extra_202 := mtarjeta_visa_consumo/ctrx_quarter]
dataset[ , extra_203 := mtarjeta_visa_consumo/mcaja_ahorro]
dataset[ , extra_204 := mtarjeta_visa_consumo/cpayroll_trx]
dataset[ , extra_205 := mtarjeta_visa_consumo/mcuentas_saldo]
dataset[ , extra_206 := mtarjeta_visa_consumo/ctarjeta_visa_transacciones]
dataset[ , extra_207 := mtarjeta_visa_consumo/mpayroll]
dataset[ , extra_208 := mtarjeta_visa_consumo/mprestamos_personales]
dataset[ , extra_209 := mtarjeta_visa_consumo/mpayroll_lag1]
dataset[ , extra_210 := mtarjeta_visa_consumo/mpasivos_margen_lag1]
dataset[ , extra_211 := mtarjeta_visa_consumo/mtarjeta_visa_consumo]
dataset[ , extra_212 := mtarjeta_visa_consumo/mactivos_margen]
dataset[ , extra_213 := mtarjeta_visa_consumo/mcaja_ahorro_lag1]
dataset[ , extra_214 := mtarjeta_visa_consumo/Visa_mconsumototal]
dataset[ , extra_215 := mtarjeta_visa_consumo/Visa_mpagominimo_lag1]
dataset[ , extra_216 := mtarjeta_visa_consumo/cpayroll_trx_lag1]
dataset[ , extra_217 := mtarjeta_visa_consumo/Visa_msaldototal]
dataset[ , extra_218 := mtarjeta_visa_consumo/Visa_mpagominimo]
dataset[ , extra_219 := mtarjeta_visa_consumo/mactivos_margen_delta1]
dataset[ , extra_220 := mtarjeta_visa_consumo/Visa_msaldopesos]
dataset[ , extra_221 := mactivos_margen/ctrx_quarter_lag1]
dataset[ , extra_222 := mactivos_margen/ctrx_quarter]
dataset[ , extra_223 := mactivos_margen/mcaja_ahorro]
dataset[ , extra_224 := mactivos_margen/cpayroll_trx]
dataset[ , extra_225 := mactivos_margen/mcuentas_saldo]
dataset[ , extra_226 := mactivos_margen/ctarjeta_visa_transacciones]
dataset[ , extra_227 := mactivos_margen/mpayroll]
dataset[ , extra_228 := mactivos_margen/mprestamos_personales]
dataset[ , extra_229 := mactivos_margen/mpayroll_lag1]
dataset[ , extra_230 := mactivos_margen/mpasivos_margen_lag1]
dataset[ , extra_231 := mactivos_margen/mtarjeta_visa_consumo]
dataset[ , extra_232 := mactivos_margen/mactivos_margen]
dataset[ , extra_233 := mactivos_margen/mcaja_ahorro_lag1]
dataset[ , extra_234 := mactivos_margen/Visa_mconsumototal]
dataset[ , extra_235 := mactivos_margen/Visa_mpagominimo_lag1]
dataset[ , extra_236 := mactivos_margen/cpayroll_trx_lag1]
dataset[ , extra_237 := mactivos_margen/Visa_msaldototal]
dataset[ , extra_238 := mactivos_margen/Visa_mpagominimo]
dataset[ , extra_239 := mactivos_margen/mactivos_margen_delta1]
dataset[ , extra_240 := mactivos_margen/Visa_msaldopesos]
dataset[ , extra_241 := mcuenta_debitos_automaticos/ctrx_quarter_lag1]
dataset[ , extra_242 := mcuenta_debitos_automaticos/ctrx_quarter]
dataset[ , extra_243 := mcuenta_debitos_automaticos/mcaja_ahorro]
dataset[ , extra_244 := mcuenta_debitos_automaticos/cpayroll_trx]
dataset[ , extra_245 := mcuenta_debitos_automaticos/mcuentas_saldo]
dataset[ , extra_246 := mcuenta_debitos_automaticos/ctarjeta_visa_transacciones]
dataset[ , extra_247 := mcuenta_debitos_automaticos/mpayroll]
dataset[ , extra_248 := mcuenta_debitos_automaticos/mprestamos_personales]
dataset[ , extra_249 := mcuenta_debitos_automaticos/mpayroll_lag1]
dataset[ , extra_250 := mcuenta_debitos_automaticos/mpasivos_margen_lag1]
dataset[ , extra_251 := mcuenta_debitos_automaticos/mtarjeta_visa_consumo]
dataset[ , extra_252 := mcuenta_debitos_automaticos/mactivos_margen]
dataset[ , extra_253 := mcuenta_debitos_automaticos/mcaja_ahorro_lag1]
dataset[ , extra_254 := mcuenta_debitos_automaticos/Visa_mconsumototal]
dataset[ , extra_255 := mcuenta_debitos_automaticos/Visa_mpagominimo_lag1]
dataset[ , extra_256 := mcuenta_debitos_automaticos/cpayroll_trx_lag1]
dataset[ , extra_257 := mcuenta_debitos_automaticos/Visa_msaldototal]
dataset[ , extra_258 := mcuenta_debitos_automaticos/Visa_mpagominimo]
dataset[ , extra_259 := mcuenta_debitos_automaticos/mactivos_margen_delta1]
dataset[ , extra_260 := mcuenta_debitos_automaticos/Visa_msaldopesos]
dataset[ , extra_261 := mcuenta_debitos_automaticos/mcomisiones_lag1]
dataset[ , extra_262 := mcuenta_debitos_automaticos/Visa_msaldopesos_lag1]
dataset[ , extra_263 := mcuenta_debitos_automaticos/mrentabilidad_annual_lag1]
dataset[ , extra_264 := mcuenta_debitos_automaticos/ccomisiones_mantenimiento_delta1]
dataset[ , extra_265 := mcuenta_debitos_automaticos/mpasivos_margen]
dataset[ , extra_266 := mcuenta_debitos_automaticos/Visa_mpagospesos_lag1]
dataset[ , extra_267 := mcuenta_debitos_automaticos/Visa_fechaalta]
dataset[ , extra_268 := mcuenta_debitos_automaticos/thomebanking]
dataset[ , extra_269 := mcuenta_debitos_automaticos/mpasivos_margen_delta1]
dataset[ , extra_270 := mcuenta_debitos_automaticos/mcuenta_debitos_automaticos]
dataset[ , extra_271 := mcuenta_debitos_automaticos/ctrx_quarter_delta1]
dataset[ , extra_272 := mcuenta_debitos_automaticos/mactivos_margen_lag1]
dataset[ , extra_273 := mcuenta_debitos_automaticos/mprestamos_personales_lag1]
dataset[ , extra_274 := mcuenta_debitos_automaticos/mcomisiones_otras_lag1]
dataset[ , extra_275 := mcuenta_debitos_automaticos/cproductos]
dataset[ , extra_276 := mcuenta_debitos_automaticos/chomebanking_transacciones]
dataset[ , extra_277 := mcuenta_debitos_automaticos/mprestamos_personales_delta1]
dataset[ , extra_278 := mcuenta_debitos_automaticos/Master_status]
dataset[ , extra_279 := mcuenta_debitos_automaticos/Master_fechaalta]
dataset[ , extra_280 := mcuenta_debitos_automaticos/mrentabilidad_lag1]
dataset[ , extra_281 := mcuenta_debitos_automaticos/cproductos_delta1]
dataset[ , extra_282 := mcuenta_debitos_automaticos/mcuentas_saldo_delta1]
dataset[ , extra_283 := mcuenta_debitos_automaticos/chomebanking_transacciones_lag1]
dataset[ , extra_284 := mcuenta_debitos_automaticos/mcomisiones_mantenimiento]
dataset[ , extra_285 := mcuenta_debitos_automaticos/mcomisiones_mantenimiento_delta1]
dataset[ , extra_286 := mcuenta_debitos_automaticos/mpayroll_delta1]
dataset[ , extra_287 := mcuenta_debitos_automaticos/mrentabilidad_annual]
dataset[ , extra_288 := mcuenta_debitos_automaticos/mrentabilidad]

  

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
