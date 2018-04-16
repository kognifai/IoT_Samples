using CsvHelper;
using Kognifai.Serialization;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace M2MqttExampleClient
{
    public static class CsvFileReader
    {
        public static IEnumerable<SensorMetadata> ReadDataFromCsvFile(string filePath)
        {
            var text = File.ReadAllText(filePath);

            using (var csvReader = new CsvReader(new StringReader(text)))
            {

                List<SensorMetadata> sensorMetadata = new List<SensorMetadata>();
                var sensorData = csvReader.GetRecords<SensorData>();
                foreach (var data in sensorData)
                {
                    SensorMetadata sensorMeta = new SensorMetadata
                    {
                        ExternalId = data.ExternalId
                    };                   
                    sensorMeta.Properties.Add("PollFrequency", new Property(data.ExternalPollFrequency.ToString()));
                    sensorMeta.Properties.Add("SensorReadingType", new Property(data.ExternalReadingType.ToString()));
                    sensorMetadata.Add(sensorMeta);
                }
                return sensorMetadata;
            }

        }
    }
}
