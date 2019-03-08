#include <iostream>
#include <fstream>
#include <string>

// boost
#include <boost/algorithm/string/classification.hpp> // Include for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // Include for boost::split
#include <boost/filesystem.hpp> // Include for boost::filesystem::create_directories
                                //         and boost::filesystem::exists

// mapnik
#include <mapnik/datasource_cache.hpp>
#include <mapnik/load_map.hpp>
#include <mapnik/map.hpp>

// mapnik-vector-tile
#include "vector_tile_compression.hpp"
#include "vector_tile_merc_tile.hpp"
#include "vector_tile_processor.hpp"

struct tile_position_range {
  std::uint64_t minz;
  std::uint64_t maxz;
  std::uint64_t minx;
  std::uint64_t maxx;
  std::uint64_t miny;
  std::uint64_t maxy;
};

const std::string COMPRESSION_NONE = "none";
const std::string COMPRESSION_ZLIB = "zlib";
const std::string COMPRESSION_GZIP = "gzip";

const std::string COMPRESSION_STRATEGY_FILTERED = "FILTERED";
const std::string COMPRESSION_STRATEGY_HUFFMAN_ONLY = "HUFFMAN_ONLY";
const std::string COMPRESSION_STRATEGY_RLE = "RLE";
const std::string COMPRESSION_STRATEGY_FIXED = "FIXED";
const std::string COMPRESSION_STRATEGY_DEFAULT = "DEFAULT";

void write_tile(const std::string& file_path, const std::string& buffer) {
    std::ofstream stream{file_path, std::ios_base::out | std::ios_base::binary};
    if (!stream) {
        throw std::runtime_error{std::string{"Error while opening the file: '"} + file_path + "'"};
    }
    stream.exceptions(std::ifstream::failbit);
    stream.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    stream.close();
    std::cout << file_path << std::endl;
}

void write_tile(const std::string& file_path, const char* const buffer, const std::size_t& size) {
    std::ofstream stream{file_path, std::ios_base::out | std::ios_base::binary};
    if (!stream) {
        throw std::runtime_error{std::string{"Error while opening the file: '"} + file_path + "'"};
    }
    stream.exceptions(std::ifstream::failbit);
    stream.write(buffer, static_cast<std::streamsize>(size));
    stream.close();
    std::cout << file_path << std::endl;
}

void usage() {
    std::cout <<
    "Usage: " << 
    std::endl << std::endl <<
    "vtiles-create " << std::endl <<
    "    minimum zoom level" << std::endl <<
    "    maximum zoom level" << std::endl <<
    "    minimum x (at min. zoom)" << std::endl <<
    "    maximum x (at min. zoom)" << std::endl <<
    "    minimum y (at min. zoom)" << std::endl <<
    "    maximum y (at min. zoom)" << std::endl <<
    "    output directory path" << std::endl <<
    "    mapnik xml stylesheet path" << std::endl <<
    "    mapnik datasource plugin file paths (comma-separated)" << std::endl <<
    "    compression (none, zlib, gzip) (default: none)" << std::endl <<
    "    compression level (0 no compression to 9 maximum compression) (default: 0)" << std::endl <<
    "    compression strategy (FILTERED, HUFFMAN_ONLY, RLE, FIXED, DEFAULT) (default: DEFAULT)" << std::endl;
}

tile_position_range validate_tile_position_range(int& argc, char** argv) {
    if (argc < 2) {
        usage();
        std::string message = "missing minimum zoom parameter";
        throw std::invalid_argument(message);
    }
    if (argc < 3) {
        usage();
        std::string message = "missing maximum zoom parameter";
        throw std::invalid_argument(message);
    }
    if (argc < 4) {
        usage();
        std::string message = "missing minimum x parameter";
        throw std::invalid_argument(message);
    }
    if (argc < 5) {
        usage();
        std::string message = "missing maximum x parameter";
        throw std::invalid_argument(message);
    }
    if (argc < 6) {
        usage();
        std::string message = "missing minimum y parameter";
        throw std::invalid_argument(message);
    }
    if (argc < 7) {
        usage();
        std::string message = "missing maximum y parameter";
        throw std::invalid_argument(message);
    }

    tile_position_range range = {
        std::stoul(argv[1]), // minimum zoom level
        std::stoul(argv[2]), // maximum zoom level
        std::stoul(argv[3]), // minimum x (at min. zoom)
        std::stoul(argv[4]), // maximum x (at min. zoom)
        std::stoul(argv[5]), // minimum y (at min. zoom)
        std::stoul(argv[6])  // maximum y (at min. zoom)
    };

    if (range.minz > range.maxz) {
        std::string message = "minz (" + std::to_string(range.minz) + ") " +
        "must be lower or equals to maxz (" + std::to_string(range.maxz) + ")";
        throw std::invalid_argument(message);
    }
    if (range.minx > range.maxx) {
        std::string message = "minx (" + std::to_string(range.minx) + ") " +
        "must be lower or equals to maxx (" + std::to_string(range.maxx) + ")";
        throw std::invalid_argument(message);
    }
    if (range.miny > range.maxy) {
        std::string message = "miny (" + std::to_string(range.miny) + ") " +
        "must be lower or equals to maxy (" + std::to_string(range.maxy) + ")";
        throw std::invalid_argument(message);
    }
    return range;
}

std::string validate_output_directory_path(int& argc, char** argv) {
    if (argc < 8) {
        usage();
        std::string message = "missing output directory path parameter";
        throw std::invalid_argument(message);
    }
    // directory path where vector tiles are created
    std::string output_directory_path = argv[7];
    return output_directory_path;
}

std::string validate_stylesheet_path(int& argc, char** argv) {
    if (argc < 9) {
        usage();
        std::string message = "missing mapnik stylesheet parameter";
        throw std::invalid_argument(message);
    }
    // file path to the mapnik xml stylesheet
    std::string stylesheet_path = argv[8];
    if ( !boost::filesystem::exists(stylesheet_path)) {
        std::string message = "mapnik stylesheet file " + stylesheet_path + " not found";
        throw std::invalid_argument(message);
    }
    return stylesheet_path;
}

std::vector<std::string> validate_plugins(int& argc, char** argv) {
    if (argc < 10) {
        usage();
        std::string message = "mapnik plugins parameter";
        throw std::invalid_argument(message);
    }
    // mapnik datasource plugin paths (comma-separated)
    std::string datasources_plugins_paths = argv[9];
    std::vector<std::string> datasources_plugins;
    if (!datasources_plugins_paths.empty()) {
        boost::split(datasources_plugins, datasources_plugins_paths, boost::is_any_of(","), boost::token_compress_on);
    }
    return datasources_plugins;
}

std::string validate_compression(int& argc, char** argv) {
    std::string compression = COMPRESSION_NONE;
    if (argc >= 11) {
        if (argv[10] == COMPRESSION_ZLIB) {
            compression = COMPRESSION_ZLIB;
        } else if (argv[10] == COMPRESSION_GZIP) {
            compression = COMPRESSION_GZIP;
        } else if (argv[10] == COMPRESSION_NONE) {
            compression = COMPRESSION_NONE;
        } else {
            std::string message = "compression must be one of the following strings: none, zlib, gzip";
            throw std::invalid_argument(message);
        }
    }
    return compression;
}

int validate_compression_level(int& argc, char** argv) {
    int compression_level = 0;
    if (argc >= 12) {
        compression_level = std::stoi(argv[11]);
        if (compression_level < 0 || compression_level > 9) {
            std::string message = "compression level must be between 0 and 9";
            throw std::invalid_argument(message);
        }
    }
    return compression_level;
}

int validate_compression_strategy(int& argc, char** argv) {
    int compression_strategy = Z_DEFAULT_STRATEGY;
    if (argc >= 13) {
        if (argv[12] == COMPRESSION_STRATEGY_FILTERED) {
            compression_strategy = Z_FILTERED;
        } else if (argv[12] == COMPRESSION_STRATEGY_HUFFMAN_ONLY) {
            compression_strategy = Z_HUFFMAN_ONLY;
        } else if (argv[12] == COMPRESSION_STRATEGY_RLE) {
            compression_strategy = Z_RLE;
        } else if (argv[12] == COMPRESSION_STRATEGY_FIXED) {
            compression_strategy = Z_FIXED;
        } else if (argv[12] == COMPRESSION_STRATEGY_DEFAULT) {
            compression_strategy = Z_DEFAULT_STRATEGY;
        } else {
            throw std::invalid_argument("compression strategy must be one of the following strings: FILTERED, HUFFMAN_ONLY, RLE, FIXED, DEFAULT");
        }
    }
    return compression_strategy;
}

int main(int argc, char** argv) {
    // validate tile position range parameters
    tile_position_range range = validate_tile_position_range(argc, argv);
    // directory path where vector tiles are created
    std::string output_directory_path = validate_output_directory_path(argc, argv);
    // file path to the mapnik xml stylesheet
    std::string stylesheet_path = validate_stylesheet_path(argc, argv);
    // mapnik datasource plugin paths
    std::vector<std::string> datasources_plugins = validate_plugins(argc, argv);
    // compression
    std::string compression = validate_compression(argc, argv);
    int compression_level = validate_compression_level(argc, argv);
    int compression_strategy = validate_compression_strategy(argc, argv);

    // register datasources plugins
    for (const std::string& datasource_plugin : datasources_plugins) {
        std::cout << "Registering " << datasource_plugin << std::endl;
        mapnik::datasource_cache::instance().register_datasource(datasource_plugin);
    }

    // load map
    const std::string projection = "+init=epsg:3857";
    mapnik::Map map(256, 256, projection);
    mapnik::load_map(map, stylesheet_path);

    // create vector tile renderer
    mapnik::vector_tile_impl::processor renderer(map);

    // for each zoom level
    for(auto z = range.minz; z <= range.maxz; z++) {
        bool is_first_zoom_level = z == range.minz;
        if (!is_first_zoom_level) {
            range.minx = 2 * range.minx;
            range.miny = 2 * range.miny;
            range.maxx = 2 * range.maxx + 1;
            range.maxy = 2 * range.maxy + 1;
        }

        // for each x
        for(auto x = range.minx; x <= range.maxx; x++) {
            // create the .../z/x/ directory
            std::string zoom_x_directory_path = output_directory_path + std::to_string(z) + "/" + std::to_string(x) + "/";
            boost::filesystem::path dir(zoom_x_directory_path);
            if (boost::filesystem::create_directories(dir)) {
                std::cout << zoom_x_directory_path << std::endl;
            }

            // for each y
            for(auto y = range.miny; y <= range.maxy; y++) {
                // create tile
                mapnik::vector_tile_impl::merc_tile tile = renderer.create_tile(x, y, z);

                std::string file_path = zoom_x_directory_path + std::to_string(y) + ".mvt";

                auto tile_data = tile.data();
                auto tile_size = tile.size();

                // if compression is specified
                if (compression != COMPRESSION_NONE) {
                    bool gzip = compression == COMPRESSION_GZIP;
                    std::unique_ptr<std::string> compressed = std::make_unique<std::string>();
                    mapnik::vector_tile_impl::zlib_compress(tile_data, *compressed, gzip, compression_level, compression_strategy);
                    write_tile(file_path, *compressed);
                } else {
                    write_tile(file_path, tile_data, tile_size);
                }

                // clear the tile
                tile.clear();
            }
        }
    }
}