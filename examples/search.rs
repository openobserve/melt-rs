use melt_rs::index::SearchIndex;
use std::time;

fn main() {
    let mut index = SearchIndex::default();
    for i in 1..1_000_000 {
        let _ = index.add(
            format!(
                "Long and winding text where ever it leads its my fault oh no{}",
                i
            )
            .as_str(),
        );
    }
    for i in 1_000_001..2_000_001 {
        let _ = index.add(format!("Wrong wrong is the key{}", i).as_str());
    }
    for i in 3_000_001..4_000_001 {
        let _ = index.add(format!("There has been some improper way to do this{}", i).as_str());
    }
    for i in 2_000_001..3_000_001 {
        let _ = index.add(format!("World is ending and I dont like it at all, there would be sa time for this and that and where does it evolve around the sun so bright and big{}", i).as_str());
    }

    for i in 1..5 {
        index.add(format!("Ankur is doing fine - {}", i).as_str());
    }
    // index.add("Ankur is doing okay");
    // index.add("Ankur is doing okay");
    // index.add("Ankur is doing okay");

    println!("Start serializing to disk");
    index.serialize_to_file("./index.bin").unwrap();
    println!("Done serializing to disk");

    let start = time::Instant::now();
    let from_disk_index = SearchIndex::deserialize_from_file("./index.bin").unwrap();
    let result = from_disk_index.search("Ankur is", true);
    println!("Result from disk: {:?}", result);

    let duration = start.elapsed();
    println!("Time elapsed in expensive_function() is: {:?}", duration);
}
