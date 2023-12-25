# nimtinydb

`nimble install nimtinydb`

## Overview
nimtinydb is a loose port of the TinyDB Python library (https://tinydb.readthedocs.io/en/latest).
It is a lightweight, file-based JSON database designed for simplicity and ease of use, ideal for small projects and quick mockups.

## Quick Links

- [Documentation](#)
- [Examples](#)
- [API Reference](#)

## Features

- Lightweight, file-based JSON database
- Simple API for creating and opening databases
- Supports various data operations: insert, retrieve, update, delete
- Compound and basic queries with intuitive syntax
- Exception handling for robust error management
- Extensible for custom use-cases

## Usage

To use Nim TinyDB, first install the package using Nimble and then import it into your Nim project.

### Examples

```
import nimtinydb

# Create a new TinyDB instance
# If the database file does not exist, it will be created
let song_db = newTinyDB("path/to/song_db.json")

# Insert a new song into the database
proc insertSong(songData: JsonNode): tuple[ok: bool, err: string, doc_id: Option[int]] =
  song_db.insert(songData)

# Retrieve a song by its ID
proc getSong(songId: string): tuple[ok: bool, err: string, song: Option[JsonNode]] =
  let query = Where("id") == %songId
  let result = song_db.search(query)
  if result.ok and result.vals.isSome():
    return (true, "", some(result.vals.get[0].item))
  else:
    return (false, "Song not found", none(JsonNode))
```


## Testing

To run tests, use `nimble test`. The test suite covers scenarios like path recognition, file/directory existence, error handling, and path concatenation.

## Contributing

Contributions are welcome! Please read the `CONTRIBUTING.md` file for guidelines.

## License

Nim TinyDB is released under the MIT License. See the `LICENSE` file for more details.

---

The above template is structured to provide a clear overview, installation instructions, feature list, usage examples, testing information, contributing guidelines, and licensing information. Adjustments can be made based on specific requirements or additional content.

`nimble install nimtinydb`

## Overview

Nim TinyDB is a lightweight, file-based JSON database designed for simplicity and ease of use. It allows users to create, open, and manipulate databases with straightforward Nim procedures. Ideal for small projects needing a simple data storage solution.

## Quick Links

- [Documentation](#)
- [Examples](#)
- [API Reference](#)

## Features

- Lightweight, file-based JSON database
- Simple API for creating and opening databases
- Supports various data operations: insert, retrieve, update, delete
- Compound and basic queries with intuitive syntax
- Exception handling for robust error management
- Extensible for custom use-cases

## Usage

To use Nim TinyDB, first install the package using Nimble and then import it into your Nim project.

### Examples

For detailed examples, refer to the `examples` directory in the repository.

## Testing

To run tests, use `nimble test`. The test suite covers scenarios like path recognition, file/directory existence, error handling, and path concatenation.

## Contributing

Contributions are welcome! Please read the `CONTRIBUTING.md` file for guidelines.

## License

Nim TinyDB is released under the MIT License. See the `LICENSE` file for more details.

---

The above template is structured to provide a clear overview, installation instructions, feature list, usage examples, testing information, contributing guidelines, and licensing information. Adjustments can be made based on specific requirements or additional content.