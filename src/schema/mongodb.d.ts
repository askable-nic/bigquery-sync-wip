import { ObjectId } from "mongodb";

export type BookingSubmissionDb = {
  _id: ObjectId;
  created?: number;
  updated?: number;
  status_updated?: number;
  eligibility?: number;
  _user_id?: ObjectId;
  _booking_id?: ObjectId;
  status?: number;
};
