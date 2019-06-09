#include <fstream>
#include <iostream>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Decoder.hh"
#include "avro/Generic.hh"
#include "avro/Stream.hh"

const std::string BOLD("\033[1m");
const std::string ENDC("\033[0m");
const std::string RED("\033[31m");
const std::string YELLOW("\033[33m");

int main(int argc, char**argv)
{
    std::cout << "AVRO Test\n" << std::endl;

    if (argc < 2)
    {
        std::cerr << BOLD << RED << "ERROR: " << ENDC << "please provide an "
                  << "input file\n" << std::endl;
        return -1;
    }

    avro::DataFileReader<avro::GenericDatum> reader(argv[1]);
    auto dataSchema = reader.dataSchema();

    // Write out data schema in JSON for grins
    std::ofstream output("data_schema.json");
    dataSchema.toJson(output);
    output.close();

    avro::GenericDatum datum(dataSchema);
    while (reader.read(datum)) 
    {
        std::cout << "Type: " << datum.type() << std::endl;
        if (datum.type() == avro::AVRO_RECORD) 
        {
            const avro::GenericRecord& r = datum.value<avro::GenericRecord>();
            std::cout << "Field-count: " << r.fieldCount() << std::endl;

            // TODO: pull out each field
        }
    }

    return 0;
}
