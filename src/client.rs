use replicatedcounter::{
    counter_client::CounterClient, AddRequest, CompareAndSetRequest, DecrementRequest,
    IncrementRequest, ReadRequest, SetRequest,
};

pub mod replicatedcounter {
    tonic::include_proto!("replicatedcounter");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CounterClient::connect("http://[::1]:50051").await?;

    // Read initial value
    let read_response = client
        .read(ReadRequest { consistency: 0 })
        .await?
        .into_inner();
    println!(
        "Initial Read: value={}, revision={}",
        read_response.value, read_response.revision
    );

    // Increment the counter by 1
    let inc_response = client
        .increment(IncrementRequest {
            amount: 0,
            options: None,
        })
        .await?
        .into_inner();
    println!(
        "After Increment: value={}, revision={}",
        inc_response.value, inc_response.revision
    );

    // Add 10 to the counter
    let add_response = client
        .add(AddRequest {
            amount: 10,
            options: None,
        })
        .await?
        .into_inner();
    println!(
        "After Add: value={}, revision={}",
        add_response.value, add_response.revision
    );

    // Set the counter to 42
    let set_response = client
        .set(SetRequest {
            value: 42,
            options: None,
        })
        .await?
        .into_inner();
    println!(
        "After Set: value={}, revision={}",
        set_response.value, set_response.revision
    );

    // Compare and set: only set to 100 if current == 42
    let cas_response = client
        .compare_and_set(CompareAndSetRequest {
            expected: 42,
            new_value: 100,
            options: None,
        })
        .await?
        .into_inner();
    println!(
        "CompareAndSet success={} new_value={} revision={}",
        cas_response.success, cas_response.value, cas_response.revision
    );

    // Decrement the counter by 2
    let dec_response = client
        .decrement(DecrementRequest {
            amount: 2,
            options: None,
        })
        .await?
        .into_inner();
    println!(
        "After Decrement: value={}, revision={}",
        dec_response.value, dec_response.revision
    );

    // Read again to verify final value
    let final_read = client
        .read(ReadRequest { consistency: 0 })
        .await?
        .into_inner();
    println!(
        "Final Read: value={}, revision={}",
        final_read.value, final_read.revision
    );

    Ok(())
}
