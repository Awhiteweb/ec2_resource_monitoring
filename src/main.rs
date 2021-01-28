extern crate tokio;

use futures::{stream, Stream, StreamExt};
use rusoto_core::{Region, RusotoError};
use rusoto_ec2::{Ec2, Ec2Client, DescribeInstancesError, DescribeInstancesRequest, DescribeInstancesResult, Instance, Reservation, Tag};
use serde::Serialize;
use std::path::Path;
use std::result::Result;
use std::str::FromStr;
use std::vec::Vec;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

type DetailResult = Result<Option<Vec<Details>>, RusotoError<DescribeInstancesError>>;

fn region_list<'a>() -> Vec<&'a str> {
     [
        "ap-east-1",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-south-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ca-central-1",
        "eu-central-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-north-1",
        "eu-south-1",
        "me-south-1",
        "sa-east-1",
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "cn-north-1",
        "cn-northwest-1",
        "af-south-1",
     ].to_vec()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        panic!("no arguments were provided\nPlease provide a valid region or 'all' to get an output from every available region")
    }
    let region = &*args[1];
    let regions = region_list();
    if !regions.contains(&region) && region != "all" {
        panic!("The supplied region does not match any of the the available options: {},\nall", regions.join(",\n"))
    }
    run(&region).await;
    Ok(())
}

async fn run<'a>(region: &'a str) {
    let path = Path::new("instance_results.json");
    let display = path.display();
    let mut file = match File::create(&path).await {
        Err(why) => panic!("couldn't create {}: {}", display, why),
        Ok(file) => file,
    };
    let output: Vec<Details> = match region {
        "all" => process_all_regions().await,
        _ => process_single_region(region.to_string()).await
    };
    let writable = serde_json::to_string(&output).unwrap_or("".to_string());
    match file.write_all((&writable).as_bytes()).await {
        Err(why) => panic!("couldn't write to {}: {}", display, why),
        Ok(_) => println!("successfully wrote to {}", display),
    }
}

async fn process_all_regions() -> Vec<Details> {
    let mut output: Vec<Details> = Vec::new();
    for r in region_list().iter() {
        let result = process_region(r.to_string()).await;
        output.extend(result);
    }
    output
}

async fn process_single_region(region: String) -> Vec<Details> {
    process_region(region.to_string()).await
}

async fn process_region(region: String) -> Vec<Details> {
    let r = Region::from_str(&region).unwrap();
    let client = Ec2Client::new(r);
    let s = describe_instances(region, client);
    let s = s.filter_map(|v| async move { v.ok() }); // returns Option<Vec<Details>>
    let s = s.filter_map(|v| async move { v }); // returns Vec<Details>
    let s: Vec<Vec<Details>> = s.collect().await;
    s.into_iter().flatten().collect()
}

fn get_instance_request(max_items: Option<i64>) -> DescribeInstancesRequest {
    DescribeInstancesRequest {
        dry_run: None,
        filters: None,
        instance_ids: None,
        max_results: max_items,
        next_token: None
    }
}

struct RequestContext {
    client: Ec2Client,
    request: Option<DescribeInstancesRequest>,
    region: String
}

fn describe_instances(region: String, ec2_client: Ec2Client) -> impl Stream<Item = DetailResult> {
    let max_items = 25;
    let ctx = Some(RequestContext {
        client: ec2_client,
        request: Some(get_instance_request(Some(max_items))),
        region: region
    });
    stream::unfold(ctx, |ctx| async {
        if ctx.is_none() {
             return None;
        }
        let rc = ctx.unwrap();
        let c = rc.client.clone();
        let response: Result<DescribeInstancesResult, RusotoError<DescribeInstancesError>> = c.describe_instances(rc.request?).await;
        match response {
            Ok(r) => {
                let result = process_reservations(r.reservations, rc.region.clone());
                if r.next_token.is_none() {
                    return Some((Ok(result), None));
                }
                let mut req = get_instance_request(Some(25));
                req.next_token = r.next_token;

                Some((Ok(result), Some(RequestContext {
                    client: rc.client,
                    request: Some(req),
                    region: rc.region
                })))
            },
            Err(_) => None
        }
    })
}

fn process_reservations(reservations: Option<Vec<Reservation>>, region: String) -> Option<Vec<Details>> {
    match reservations {
        Some(r) => Some(r.into_iter()
            .map(|r| instance_map(r.instances, &region.clone()))
            .filter(|r| r.is_some())
            .map(|r| r.unwrap())
            .collect::<Vec<Vec<Details>>>()
            .into_iter()
            .flatten()
            .collect::<Vec<Details>>()),
        None => None
    }
}

fn instance_map<'a>(instances: Option<Vec<Instance>>, region: &'a str) -> Option<Vec<Details>> {
    let result = instances?.into_iter().map(|a| {
        let tag_map = map_tags(a.tags);
        Details {
            instance_id: a.instance_id,
            instance_type: a.instance_type,
            key_name: a.key_name,
            launch_time: a.launch_time,
            region: region.to_string(),
            source_dest_check: a.source_dest_check,
            state: match a.state {
                Some(s) => s.name,
                _ => None
            },
            name: tag_map.name,
            project: tag_map.project,
            environment: tag_map.environment
        }
    }).collect();
    Some(result)
}

fn map_tags(tags: Option<Vec<Tag>>) -> TagMap {
    let mut tag_map = TagMap {
        project: None,
        environment: None,
        name: None
    };
    let tag_iter = tags.unwrap_or(Vec::new())
        .into_iter()
        .filter(|t| t.key == Some("Name".to_string()) || t.key == Some("Project".to_string()) || t.key == Some("Environment".to_string()));
    for val in tag_iter {
        if val.key == Some("Name".to_string()) {
            tag_map.name = val.value
        }
        else if val.key == Some("Project".to_string()) {
            tag_map.project = val.value
        }
        else if val.key == Some("Environment".to_string()) {
            tag_map.environment = val.value
        }
    }
    tag_map
}

struct TagMap {
    environment: Option<String>,
    name: Option<String>,
    project: Option<String>
}

#[derive(Serialize, Debug, Clone)]
struct Details {
    environment: Option<String>,
    instance_id: Option<String>,
    instance_type: Option<String>,
    key_name: Option<String>,
    launch_time: Option<String>,
    name: Option<String>,
    project: Option<String>,
    region: String,
    source_dest_check: Option<bool>,
    state: Option<String>
}
