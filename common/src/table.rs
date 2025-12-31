//! Defines the TableHeap structure which manages a collection of pages that stores the rows of a table.

use crate::api::{BufferPoolManager, PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::tuple::{Tuple, Schema};
use crate::page::{SlottedPage, PageType};
use std::sync::Arc;
use std::ops::DerefMut;

/// A row ID is a combination of a page ID and a slot index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page_id: PageId,
    pub slot_index: u16,
}

/// The TableHeap struct manages a collection of pages that stores the rows of a table.
pub struct TableHeap {
    bpm: Arc<dyn BufferPoolManager>,
    first_page_id: PageId,
    schema: Schema,
}

impl TableHeap {
    /// Creates a new table heap.
    pub fn new(bpm: Arc<dyn BufferPoolManager>, schema: Schema) -> Self {
        // Allocate a new page for the table heap.
        let first_page_id = {
            let mut first_page = bpm.new_page().expect("Failed to create a new page");
            let page_id = first_page.page_id();
            let mut slotted_page = SlottedPage::new(first_page.deref_mut());
            Self::initialize_page(&mut slotted_page);
            page_id
        };
        Self { bpm, first_page_id, schema }
    }

    /// Initializes a new slotted page.
    fn initialize_page(slotted_page: &mut SlottedPage) {
        let header = slotted_page.header_mut();
        header.page_type = PageType::TablePage;
        header.next_page_id = INVALID_PAGE_ID;
        header.slot_count = 0;
        header.free_space_pointer = PAGE_SIZE as u16;
    }

    /// Inserts a tuple into the table heap.
    /// Returns the RowId of the inserted tuple.
    pub fn insert_tuple(&self, tuple: &Tuple) -> Option<RowId> {
        let serialized_tuple = tuple.serialize(&self.schema);

        let mut current_page_id = self.first_page_id;
        loop {
            let mut page_guard = match self.bpm.fetch_page(current_page_id) {
                Ok(guard) => guard,
                Err(_) => return None,
            };
            let mut slotted_page = SlottedPage::new(page_guard.deref_mut());

            if let Some(slot_index) = slotted_page.insert_record(&serialized_tuple) {
                return Some(RowId {
                    page_id: current_page_id,
                    slot_index,
                });
            }

            // If there is not enough space, go to the next page.
            let next_page_id = slotted_page.header().next_page_id;
            if next_page_id == INVALID_PAGE_ID {
                // This is the last page, and it's full.
                // Allocate a new page.
                let mut new_page_guard = match self.bpm.new_page() {
                    Ok(guard) => guard,
                    Err(_) => return None,
                };
                let new_page_id = new_page_guard.page_id();
                let mut new_slotted_page = SlottedPage::new(new_page_guard.deref_mut());
                Self::initialize_page(&mut new_slotted_page);

                // Link the new page to the current page.
                slotted_page.header_mut().next_page_id = new_page_id;

                // Insert the tuple into the new page.
                if let Some(slot_index) = new_slotted_page.insert_record(&serialized_tuple) {
                    return Some(RowId {
                        page_id: new_page_id,
                        slot_index,
                    });
                } else {
                    // This should not happen, as the new page should have enough space.
                    return None;
                }
            } else {
                current_page_id = next_page_id;
            }
        }
    }

    /// Gets a tuple from the table heap given its RowId.
    pub fn get_tuple(&self, row_id: RowId) -> Option<Tuple> {
        let mut page_guard = match self.bpm.fetch_page(row_id.page_id) {
            Ok(guard) => guard,
            Err(_) => return None,
        };
        let slotted_page = SlottedPage::new(page_guard.deref_mut());
        let record = slotted_page.get_record(row_id.slot_index);
        Some(Tuple::deserialize(record, &self.schema))
    }
}

