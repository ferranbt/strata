//! A load harness: drive one operation repeatedly and report sustained throughput.
//!
//! Not a criterion benchmark — the operations measured here (inserting into
//! postgres, clickhouse, …) mutate persistent state, so batches aren't independent
//! and throughput legitimately shifts as the table grows. What we want is sustained
//! rows/sec, not `ns/iter`.
//!
//! The driver knows nothing about writes: an operation is any async
//! `(offset, batch) -> rows` closure, so a read op plugs in the same way.

use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::Result;

/// When to stop driving the operation.
#[derive(Debug, Clone, Copy)]
pub enum Stop {
    /// Stop once this many rows have been *measured*, warmup excluded.
    Rows(u64),
    /// Stop after this much wall time.
    After(Duration),
}

#[derive(Debug, Clone, Copy)]
pub struct Plan {
    pub batch: usize,
    pub stop: Stop,
    /// Batches driven but not measured. The first write creates the table, so
    /// without this a short run mostly measures DDL.
    pub warmup: usize,
}

impl Plan {
    pub fn new(batch: usize, stop: Stop) -> Self {
        Plan {
            batch,
            stop,
            warmup: 1,
        }
    }

    pub fn warmup(mut self, batches: usize) -> Self {
        self.warmup = batches;
        self
    }
}

/// One measured batch.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub rows: u64,
    pub elapsed: Duration,
    /// Rows already written before this batch.
    pub offset: u64,
}

pub struct Report {
    pub samples: Vec<Sample>,
    pub wall: Duration,
}

impl Report {
    pub fn rows(&self) -> u64 {
        self.samples.iter().map(|s| s.rows).sum()
    }

    pub fn rows_per_sec(&self) -> f64 {
        let secs = self.wall.as_secs_f64();
        if secs > 0.0 {
            self.rows() as f64 / secs
        } else {
            0.0
        }
    }
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} rows in {:.2?} — {:.0} rows/s ({} batches)",
            self.rows(),
            self.wall,
            self.rows_per_sec(),
            self.samples.len(),
        )
    }
}

/// Drive `op` until the plan's [`Stop`] condition, timing each batch.
///
/// `op` is called as `(offset, batch) -> rows`, where `offset` is the number of rows
/// already written — thread it into the data generator so each batch produces
/// distinct rows, otherwise a keyed write dedups and measures nothing.
pub async fn run<F, Fut>(plan: &Plan, mut op: F) -> Result<Report>
where
    F: FnMut(u64, usize) -> Fut,
    Fut: Future<Output = Result<u64>>,
{
    let mut offset = 0u64;
    for _ in 0..plan.warmup {
        offset += op(offset, plan.batch).await?;
    }

    let mut samples = Vec::new();
    let mut measured = 0u64;
    let start = Instant::now();
    loop {
        match plan.stop {
            Stop::Rows(target) if measured >= target => break,
            Stop::After(limit) if start.elapsed() >= limit => break,
            _ => {}
        }
        let batch_start = Instant::now();
        let rows = op(offset, plan.batch).await?;
        samples.push(Sample {
            rows,
            elapsed: batch_start.elapsed(),
            offset,
        });
        offset += rows;
        measured += rows;
        // A no-op batch would otherwise spin forever.
        if rows == 0 {
            break;
        }
    }

    Ok(Report {
        samples,
        wall: start.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stops_on_row_target_and_excludes_warmup() -> Result<()> {
        let mut calls = 0;
        let report = run(&Plan::new(10, Stop::Rows(50)).warmup(2), |_, batch| {
            calls += 1;
            async move { Ok(batch as u64) }
        })
        .await?;

        // 2 warmup batches are driven but not measured; 5 more reach the 50 target.
        assert_eq!(calls, 7);
        assert_eq!(report.rows(), 50);
        assert_eq!(report.samples.len(), 5);
        // Measurement starts after warmup, so the first sample is already at 20.
        assert_eq!(report.samples[0].offset, 20);
        Ok(())
    }

    #[tokio::test]
    async fn stops_on_duration() -> Result<()> {
        let report = run(
            &Plan::new(1, Stop::After(Duration::from_millis(50))).warmup(0),
            |_, batch| async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok(batch as u64)
            },
        )
        .await?;

        assert!(report.wall >= Duration::from_millis(50));
        assert!(!report.samples.is_empty());
        Ok(())
    }

}
