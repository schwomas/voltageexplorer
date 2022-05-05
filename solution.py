import apache_beam as beam
from apache_beam import window
import re
import json
from datetime import datetime
import numbers

# Path to file on my home (windows) machine.
datafile ="D:\\em\\1_day.jsonl";

def sumProd(production):
    return sum([ x for x in production.values() if isinstance(x, numbers.Number) ] )

def getTimestamp(thjson):
    dt = thjson["datetime"]
    parsedtime = datetime.fromisoformat(dt)
    return int(parsedtime.timestamp())
def calculateConsumption(prodexchange):
    return prodexchange['production'][0] -prodexchange['export'][0] if prodexchange['export'] else prodexchange['production'][0] 


def getFlow(flowdata):
  flow = flowdata["sortedCountryCodes"]
  items = []
  # Assuming alll flows go either to or from a zone starting with DK

   # "Negative values indicate export of electricity out of the area to the connected area, and positive values indicate import."
  if "->DK" in flow:
    items.append( ( flow.split("->")[1], -1.0*flowdata["netFlow"]  ) ) 
  if flow.startswith("DK"):
    items.append( ( flow.split("->")[0], flowdata["netFlow"]  ) ) 
  return items

class AddTimestampDoFn(beam.DoFn):
  def process(self, element):
    unix_timestamp = getTimestamp(element)
    yield beam.window.TimestampedValue(element, unix_timestamp)

class BuildRecordFn(beam.DoFn):
  def process(self, element,  window=beam.DoFn.WindowParam):
    #window_start = window.start.to_utc_datetime()
    window_end = window.end.to_utc_datetime()
    yield (element[0], element[1], window_end )


p = beam.Pipeline()
lines = p | "read" >> beam.io.ReadFromText(datafile)
jsons = lines | "tojson" >> beam.Map(lambda line: json.loads(line))
timestamped =  jsons | "addTimestamp" >> beam.ParDo(AddTimestampDoFn())
fixed_windowed_jsons  = timestamped | 'window' >> beam.WindowInto(window.FixedWindows(3600))

#production fork
windowedProduction = fixed_windowed_jsons | 'productionOnly' >> beam.Filter( lambda j: j["kind"] == "ElectricityProduction" )
productionOverTypes =   windowedProduction | 'sumproductionByZone' >> beam.Map( lambda v: (v["zone_key"],  sumProd( v["data"]["production"] )  ))
windowedProductionByZone =   productionOverTypes | 'Group and sum' >> beam.CombinePerKey(sum)

# exchange fork
windowedExchange = fixed_windowed_jsons | 'exchangeOnly' >> beam.Filter( lambda j: j["kind"] == "ElectricityExchange" )
netExchangeDk = windowedExchange | 'getting net values from dk zones' >> beam.FlatMap( lambda j: getFlow(j['data']) )
netExchangeDkPerWindow =   netExchangeDk | 'Group and sum exchange' >> beam.CombinePerKey(sum) # Assuming (wrongly) that exchange numbers describe the flow since last measurement

# join
prodexchange = ( {'production':windowedProductionByZone, 'export': netExchangeDkPerWindow}) | 'Merge' >> beam.CoGroupByKey()
withtimes = prodexchange |  'AddWindowEndTimestamp' >> (beam.ParDo(BuildRecordFn())) 
consumption = withtimes | 'calculate consumption' >> beam.Map(lambda e: ( e[0], calculateConsumption(e[1]), e[2]  ) ) 
printed = consumption | 'print' >>  beam.Map(lambda e: print( str(e[0]) + "\t"+ str(e[2]) +"\t" + f'{e[1]:.2f}'  ))

p.run()

