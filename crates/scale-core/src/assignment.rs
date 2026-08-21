use thiserror::Error;

/// Applies the ordered sticky assignment rule to one owner map.
///
/// Surviving owners keep their partitions. The rule trims excess partitions
/// from the highest partition indexes. It fills holes in owner order.
///
/// `termination_order` lists owners from first terminated to last terminated.
/// The output buffers must match the configured partition and owner bounds.
///
/// # Errors
///
/// Returns an error when an input or output does not represent the bounds.
pub fn sticky_assignment(
    current: &[u32],
    target_count: u32,
    termination_order: &[u32],
    target: &mut [u32],
    owner_counts: &mut [u32],
    moved: &mut [bool],
) -> Result<(), AssignmentError> {
    if current.is_empty()
        || target_count == 0
        || target.len() != current.len()
        || moved.len() != current.len()
        || target_count as usize > owner_counts.len()
    {
        return Err(AssignmentError::Bounds);
    }
    let current_count = current
        .iter()
        .copied()
        .max()
        .and_then(|owner| owner.checked_add(1))
        .ok_or(AssignmentError::Owner)?;
    if current.iter().any(|owner| *owner >= current_count)
        || termination_order
            .iter()
            .filter(|owner| **owner < current_count)
            .count()
            < current_count.saturating_sub(target_count) as usize
    {
        return Err(AssignmentError::Owner);
    }
    let owner_count = target_count.min(current.len() as u32);
    owner_counts.fill(0);
    target.copy_from_slice(current);
    let removed_count = current_count.saturating_sub(owner_count) as usize;
    for &owner in termination_order
        .iter()
        .filter(|owner| **owner < current_count)
        .take(removed_count)
    {
        for target_owner in target.iter_mut().filter(|value| **value == owner) {
            *target_owner = u32::MAX;
        }
    }
    for target_owner in target.iter_mut().filter(|owner| **owner != u32::MAX) {
        let removed_before = termination_order
            .iter()
            .filter(|owner| **owner < current_count)
            .take(removed_count)
            .filter(|removed| **removed < *target_owner)
            .count() as u32;
        *target_owner -= removed_before;
        owner_counts[*target_owner as usize] += 1;
    }

    let base = current.len() as u32 / owner_count;
    let remainder = current.len() as u32 % owner_count;
    for owner in 0..owner_count {
        let desired = base + u32::from(owner < remainder);
        let mut excess = owner_counts[owner as usize].saturating_sub(desired);
        for target_owner in target.iter_mut().rev() {
            if excess == 0 {
                break;
            }
            if *target_owner == owner {
                *target_owner = u32::MAX;
                owner_counts[owner as usize] -= 1;
                excess -= 1;
            }
        }
    }
    let mut owner = 0_u32;
    for target_owner in target.iter_mut().filter(|owner| **owner == u32::MAX) {
        while owner < owner_count {
            let desired = base + u32::from(owner < remainder);
            if owner_counts[owner as usize] < desired {
                break;
            }
            owner += 1;
        }
        if owner == owner_count {
            return Err(AssignmentError::Balance);
        }
        *target_owner = owner;
        owner_counts[owner as usize] += 1;
    }
    for (was, (now, did_move)) in current.iter().zip(target.iter().zip(moved)) {
        *did_move = was != now;
    }
    Ok(())
}

/// An invalid ordered assignment input.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AssignmentError {
    /// A slice or count does not match the configured bound.
    #[error("assignment bounds do not match")]
    Bounds,
    /// An owner is outside the current assignment.
    #[error("assignment owner is invalid")]
    Owner,
    /// The assignment cannot fill a balanced target.
    #[error("assignment cannot reach the balanced target")]
    Balance,
}
