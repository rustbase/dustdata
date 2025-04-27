use std::collections::HashMap;

use super::{spec::PageNumber, Page};

const PIN_LIMIT_PERCENT: f32 = 50.0;
const MAX_CACHE_SIZE: usize = 1024;

pub enum FrameFlags {
    REF = 0b001,
    DIRTY = 0b010,
    PINNED = 0b100,
}

#[derive(Clone)]
pub struct Frame {
    pub page: Page,
    pub page_number: PageNumber,
    flags: u8,
}

impl Frame {
    pub fn new(page: Page, page_number: PageNumber) -> Self {
        Self {
            page,
            page_number,
            flags: FrameFlags::REF as u8,
        }
    }

    pub fn set(&mut self, flag: FrameFlags) {
        self.flags |= flag as u8
    }

    pub fn unset(&mut self, flag: FrameFlags) {
        self.flags &= !(flag as u8)
    }

    pub fn has(&self, flag: FrameFlags) -> bool {
        self.flags & (flag as u8) != 0
    }
}

type FrameNumber = usize;

pub struct CacheBuilder {
    max_size: usize,
    pin_limit_percent: f32,
}

impl Default for CacheBuilder {
    fn default() -> Self {
        CacheBuilder::new()
    }
}

impl CacheBuilder {
    pub fn new() -> Self {
        Self {
            max_size: MAX_CACHE_SIZE,
            pin_limit_percent: PIN_LIMIT_PERCENT,
        }
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    pub fn pin_limit_percent(mut self, percent: f32) -> Self {
        self.pin_limit_percent = percent;
        self
    }

    pub fn build(self) -> Cache {
        Cache {
            buffer: Vec::new(),
            pages: HashMap::new(),
            clock_hand: 0,
            pinned: 0,
            max_size: self.max_size,
            pin_limit_percent: self.pin_limit_percent,
        }
    }
}

pub struct Cache {
    buffer: Vec<Frame>,
    pages: HashMap<PageNumber, FrameNumber>,
    clock_hand: FrameNumber,
    max_size: usize,

    pin_limit_percent: f32,
    pinned: usize,
}

impl Cache {
    pub fn contains(&self, page_number: PageNumber) -> bool {
        self.pages.contains_key(&page_number)
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn put(&mut self, page_number: PageNumber, page: Page) -> Option<Frame> {
        if let Some(idx) = self.pages.get(&page_number) {
            let entry = &mut self.buffer[*idx];

            entry.page = page;

            return None;
        }

        if self.buffer.len() < self.max_size {
            self.buffer.push(Frame::new(page, page_number));
            self.pages.insert(page_number, self.buffer.len() - 1);

            None
        } else {
            self.replace(page_number, page)
        }
    }

    pub fn get(&mut self, page_number: PageNumber) -> Option<&Page> {
        match self.pages.get(&page_number) {
            None => None,
            Some(idx) => {
                let frame = &mut self.buffer[*idx];

                frame.set(FrameFlags::REF);
                Some(&frame.page)
            }
        }
    }

    fn get_frame_mut(&mut self, page_number: PageNumber) -> Option<&mut Frame> {
        match self.pages.get(&page_number) {
            None => None,
            Some(idx) => {
                let frame = &mut self.buffer[*idx];

                Some(frame)
            }
        }
    }

    pub fn peek(&mut self, page_number: PageNumber) -> Option<&Page> {
        match self.pages.get(&page_number) {
            None => None,
            Some(idx) => Some(self.buffer.get(*idx).map(|entry| &entry.page).unwrap()),
        }
    }

    fn replace(&mut self, page_number: PageNumber, page: Page) -> Option<Frame> {
        let mut attempts = 0;

        while attempts < self.pages.len() {
            let index = self.clock_hand % self.pages.len();
            let frame = &mut self.buffer[index];

            if frame.has(FrameFlags::PINNED) {
                self.clock_hand = (self.clock_hand + 1) % self.pages.len();
                attempts += 1;
                continue;
            }

            if frame.has(FrameFlags::REF) {
                frame.unset(FrameFlags::REF);
                self.clock_hand = (self.clock_hand + 1) % self.pages.len();
                attempts += 1;

                continue;
            }

            let mut dirty_page = None;
            if frame.has(FrameFlags::DIRTY) {
                dirty_page = Some(frame.clone());
            }

            *frame = Frame::new(page, page_number);

            self.clock_hand = (self.clock_hand + 1) % self.pages.len();
            return dirty_page;
        }

        None
    }

    pub fn pin(&mut self, page_number: PageNumber) -> bool {
        let pinned_percentage = self.pinned as f32 / self.max_size as f32 * 100.0;

        if pinned_percentage >= self.pin_limit_percent {
            return false;
        }

        match self.get_frame_mut(page_number) {
            None => false,
            Some(frame) => {
                frame.set(FrameFlags::PINNED);
                self.pinned += 1;
                true
            }
        }
    }

    pub fn unpin(&mut self, page_number: PageNumber) -> bool {
        match self.get_frame_mut(page_number) {
            None => false,
            Some(frame) => {
                frame.unset(FrameFlags::PINNED);
                self.pinned -= 1;
                true
            }
        }
    }

    pub fn dirty(&mut self, page_number: PageNumber, page: Page) -> bool {
        match self.get_frame_mut(page_number) {
            None => false,
            Some(frame) => {
                frame.page = page;
                frame.set(FrameFlags::DIRTY);
                true
            }
        }
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    #[test]
    fn put_test() {
        let mut cache = CacheBuilder::new().max_size(2).build();

        cache.put(0, Page::create(0).unwrap());
        cache.put(1, Page::create(0).unwrap());

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn contains_test() {
        let mut cache = CacheBuilder::new().max_size(4).build();
        let page = Page::create(0).unwrap();
        let page1 = Page::create(0).unwrap();
        let page2 = Page::create(0).unwrap();
        let page3 = Page::create(0).unwrap();

        cache.put(0, page.clone());
        cache.put(1, page1.clone());
        cache.put(2, page2.clone());
        cache.put(3, page3.clone());

        assert!(cache.contains(0));
        assert!(cache.contains(1));
        assert!(cache.contains(2));
        assert!(cache.contains(3));
    }
}
